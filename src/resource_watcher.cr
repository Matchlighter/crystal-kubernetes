module Kubernetes
  class ResourceWatcher(T)
    @dedicated_client : HTTP::Client?
    @state : State

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
    end

    def close
      @mutex.synchronize do
        @state = State::Closing
        @dedicated_client.try(&.close)
        @dedicated_client = nil
      end
    end

    def start_watching!
      @mutex.synchronize do
        raise "Watch already active" unless @state = State::Ready
        @state = State::Watching
      end

      # This needs a dedicated HTTP connection so that we can call close() on it
      #   Calling response.body_io.close() does not work
      client = @dedicated_client ||= @k8s_client.create_http_client

      params = @params.dup
      params["watch"] = "1"
      params["resourceVersion"] ||= "0"
      params["timeoutSeconds"] ||= "6000"

      latest_response = nil

      loop do
        return nil unless @state == State::Watching

        return client.get "#{@api_path}?#{params}" do |response|
          latest_response = response
          unless response.success?
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

              if match = obj.message.match /too old resource version: \d+ \((\d+)\)/
                params["resourceVersion"] = match[1]
              end
              # If this is an error of some kind, we don't care we'll just run
              # another request starting from the last resource version we've
              # worked with.
              next
            end

            watch = Watch(T).from_json(json_string)

            # If there's a JSON parsing failure and we loop back around, we'll
            # use this resource version to pick up where we left off.
            if new_version = watch.object.metadata.resource_version.presence
              params["resourceVersion"] = new_version
            end

            yield watch
          end
        end
      rescue ex : IO::EOFError
        # Server closed the connection after the timeout
      rescue ex : IO::Error
        return nil unless @state == State::Watching

        @log.warn { ex }
        sleep 1.second # Don't hammer the server
      rescue ex : JSON::ParseException
        # This happens when the watch request times out. This is expected and
        #   not an error, so we just ignore it.
        unless ex.message.try &.includes? "Expected BeginObject but was EOF at line 1, column 1"
          @log.warn { "Cannot parse watched object: #{ex}" }
        end
      end
    ensure
      @mutex.synchronize do
        if @state == State::Watching
          @log.warn { "Exited watch loop for #{@api_path}, response = #{latest_response.inspect}" }
        else
          @log.debug { "Gracefully exited watch loop for #{@api_path}" }
        end
        @state = State::Closed
      end
    end
  end
end
