module Kubernetes
  # Internal signal used to restart a watch from a fresh snapshot when the API
  # server no longer has the requested resource version.
  class ResourceVersionExpired < Exception
  end

  class ResourceWatcher(T)
    @dedicated_client : HTTP::Client?
    @state : State
    @on_change_handlers : Array(Watch(T) -> Nil)
    @on_reset_handlers : Array(-> Nil)

    getter :api_path, :params

    enum State
      Ready
      Watching
      Closing
      Closed
    end

    def initialize(@k8s_client : Client, @api_path : String, @params : URI::Params = URI::Params.new)
      @mutex = Mutex.new
      @state = State::Ready
      @log = @k8s_client.logger
      @on_change_handlers = [] of Watch(T) -> Nil
      @on_reset_handlers = [] of -> Nil
    end

    # Register a callback that fires on each watch event (ADDED, MODIFIED, DELETED).
    # Multiple handlers can be registered; all are called in registration order.
    def on_change(&block : Watch(T) -> Nil)
      @on_change_handlers << block
    end

    # Register a callback that fires before a watch is restarted without a
    # resource version. Consumers maintaining a cache must discard their old
    # snapshot because events may have been compacted by the API server.
    def on_reset(&block : -> Nil)
      @on_reset_handlers << block
    end

    def close
      @mutex.synchronize do
        @state = State::Closing
        @dedicated_client.try(&.close)
        @dedicated_client = nil
      end
    end

    def start_watching!
      start_watching! { |_watch| }
    end

    def start_watching!(&)
      @mutex.synchronize do
        raise "Watch already active" unless @state == State::Ready
        @state = State::Watching
      end

      params = @params.dup
      params["watch"] = "1"
      params["resourceVersion"] ||= "0"
      params["timeoutSeconds"] ||= "6000"

      latest_response = nil

      loop do
        break unless client = active_client

        client.get "#{@api_path}?#{params}" do |response|
          latest_response = response
          unless response.success?
            if response.status_code == 410
              raise ResourceVersionExpired.new
            end

            if response.headers["Content-Type"]?.try(&.includes?("application/json"))
              message = JSON.parse(response.body_io)
            else
              message = response.body_io.gets_to_end
            end

            raise ClientError.new("#{response.status}: #{message}", nil, response)
          end

          loop do
            json_string = response.body_io.read_line

            parser = JSON::PullParser.new(json_string)
            kind = parser.on_key!("object") do
              parser.on_key!("kind") do
                parser.read_string
              end
            end

            if kind == "Status"
              watch = Watch(Status).from_json(json_string)
              obj = watch.object

              if obj.code == 410 || obj.reason == "Expired" || obj.message.includes?("too old resource version")
                raise ResourceVersionExpired.new
              end

              @log.warn { "Watch error for #{@api_path}: #{obj.message}" }
              next
            end

            watch = Watch(T).from_json(json_string)

            # If there's a JSON parsing failure and we loop back around, we'll
            # use this resource version to pick up where we left off.
            if new_version = watch.object.metadata.resource_version.presence
              params["resourceVersion"] = new_version
            end

            @on_change_handlers.each &.call(watch)
            yield watch
          end
        end
      rescue ex : ResourceVersionExpired
        return nil unless watching?

        # A non-zero resource version does not send the current state. Omit it
        # so the next request starts from a current snapshot with synthetic
        # ADDED events, and let cache consumers discard state that can no
        # longer be reconciled.
        params.delete("resourceVersion")
        @on_reset_handlers.each &.call
        reset_client
      rescue ex : IO::EOFError
        # Server closed the connection after the timeout
      rescue ex : IO::Error
        return nil unless watching?

        @log.warn { ex }
        reset_client
        sleep 1.second # Don't hammer the server
      rescue ex : JSON::ParseException
        # This happens when the watch request times out. This is expected and
        #   not an error, so we just ignore it.
        unless ex.message.try &.includes? "Expected BeginObject but was EOF at line 1, column 1"
          @log.warn { "Cannot parse watched object: #{ex}" }
        end
      rescue ex : ClientError
        return nil unless watching?

        @log.warn { ex }
        sleep 1.second # Don't hammer the server
      end
    ensure
      @mutex.synchronize do
        if @state == State::Watching
          @log.warn { "Exited watch loop for #{@api_path}, response = #{latest_response.inspect}" }
        else
          @log.debug { "Gracefully exited watch loop for #{@api_path}" }
        end
        @dedicated_client.try(&.close)
        @dedicated_client = nil
        @state = State::Closed
      end
    end

    private def watching?
      @mutex.synchronize { @state == State::Watching }
    end

    # Watches need a dedicated connection so close can interrupt a blocked read.
    private def active_client : HTTP::Client?
      @mutex.synchronize do
        return nil unless @state == State::Watching
        @dedicated_client ||= @k8s_client.create_http_client
      end
    end

    private def reset_client
      @mutex.synchronize do
        @dedicated_client.try(&.close)
        @dedicated_client = nil
      end
    end
  end
end
