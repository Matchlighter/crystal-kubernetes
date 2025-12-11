module Kubernetes
  # Define a new Kubernetes resource type. This can be used to specify your CRDs
  # to be able to manage your custom resources in Crystal code.
  macro define_resource(name, group, type, version = "v1", prefix = "apis", api_version = nil, kind = nil, list_type = nil, singular_name = nil, cluster_wide = false)
    {% api_version ||= "#{group}/#{version}" %}
    {% if kind == nil %}
      {% if type.resolve == ::Kubernetes::Resource %}
        {% kind = type.type_vars.first %}
      {% else %}
        {% kind = type.stringify %}
      {% end %}
    {% end %}
    {% singular_name ||= name.gsub(/s$/, "").id %}
    {% plural_method_name = name.gsub(/-/, "_") %}
    {% singular_method_name = singular_name.gsub(/-/, "_") %}

    class ::Kubernetes::Client
      def {{plural_method_name.id}}(
        {% if cluster_wide == false %}
          namespace : String? = "default",
        {% end %}
        # FIXME: Currently this is intended to be a string, but maybe we should
        # make it a Hash/NamedTuple?
        label_selector = nil,
      )
        label_selector = make_label_selector_string(label_selector)
        {% if cluster_wide == false %}
          namespace &&= "/namespaces/#{namespace}"
        {% else %}
          namespace = nil
        {% end %}
        params = URI::Params.new
        params["labelSelector"] = label_selector if label_selector
        path = "/{{prefix.id}}/{{group.id}}/{{version.id}}#{namespace}/{{name.id}}?#{params}"
        get path do |response|
          {% if list_type %}
            parse_response!(response, {{list_type}}).not_nil!
          {% else %}
            parse_response!(response, ::Kubernetes::List({{type}})).not_nil!
          {% end %}
        rescue err : ClientError
          if err.status_code == 404
            raise ClientError.new("API resource \"{{name.id}}\" not found. Did you apply the CRD to the Kubernetes control plane?", err.status, response)
          else
            raise err
          end
        end
      end

      def {{singular_method_name.id}}(
        name : String,
        {% if cluster_wide == false %}
          namespace : String = "default",
        {% end %}
        resource_version : String = ""
      )
        {% if cluster_wide == false %}
          namespace &&= "/namespaces/#{namespace}"
        {% else %}
          namespace = nil
        {% end %}

        path = "/{{prefix.id}}/{{group.id}}/{{version.id}}#{namespace}/{{name.id}}/#{name}"
        params = URI::Params{
          "resourceVersion" => resource_version,
        }

        get "#{path}?#{params}" do |response|
          parse_response(response)
        end
      end

      def apply_{{singular_method_name.id}}(
        resource : {{type}},
        spec,
        name : String = resource.metadata.name,
        {% unless cluster_wide %}
        namespace : String? = resource.metadata.namespace,
        {% end %}
        force : Bool = false,
        field_manager : String? = nil,
      )
        path = "/{{prefix.id}}/{{group.id}}/{{version.id}}{{cluster_wide ? "".id : "/namespaces/\#{namespace}".id}}/{{name.id}}/#{name}"
        params = URI::Params{
          "force" => force.to_s,
          "fieldManager" => field_manager || "k8s-cr",
        }
        metadata = {
          name: name,
          namespace: namespace,
        }
        if resource_version = resource.metadata.resource_version.presence
          metadata = metadata.merge(resourceVersion: resource_version)
        end

        response = patch "#{path}?#{params}", {
          apiVersion: resource.api_version,
          kind: resource.kind,
          metadata: metadata,
          spec: spec,
        }

        if body = response.body
          {{type}}.from_json response.body
        else
          raise "Missing response body"
        end
      end

      def apply_{{singular_method_name.id}}(
        metadata : NamedTuple | Metadata,
        api_version : String = "{{group.id}}/{{version.id}}",
        kind : String = "{{kind.id}}",
        force : Bool = false,
        field_manager : String? = nil,
        **kwargs,
      )
        case metadata
        in NamedTuple
          name = metadata[:name]
          {% if cluster_wide == false %}
            namespace = metadata[:namespace]
          {% end %}
        in Metadata
          name = metadata.name
          {% if cluster_wide == false %}
            namespace = metadata.namespace
          {% end %}
        end

        path = "/{{prefix.id}}/{{group.id}}/{{version.id}}{% if cluster_wide == false %}/namespaces/#{namespace}{% end %}/{{name.id}}/#{name}"
        params = URI::Params{
          "force" => force.to_s,
          "fieldManager" => field_manager || "k8s-cr",
        }
        response = patch "#{path}?#{params}", {
          apiVersion: api_version,
          kind: kind,
          metadata: metadata,
        }.merge(kwargs)

        parse_response(response)
      end

      def patch_{{singular_method_name.id}}(name : String, {% if cluster_wide == false %}namespace, {% end %}**kwargs)
        path = "/{{prefix.id}}/{{group.id}}/{{version.id}}{% if cluster_wide == false %}/namespaces/#{namespace}{% end %}/{{name.id}}/#{name}"
        headers = HTTP::Headers{
          "Content-Type" =>  "application/merge-patch+json",
        }

        response = raw_patch path, kwargs.to_json, headers: headers
        parse_response(response)
      end

      def patch_{{singular_method_name.id}}_subresource(name : String, subresource : String{% if cluster_wide == false %}, namespace : String = "default"{% end %}, **args)
        path = "/{{prefix.id}}/{{group.id}}/{{version.id}}{% if cluster_wide == false %}/namespaces/#{namespace}{% end %}/{{name.id}}/#{name}/#{subresource}"
        headers = HTTP::Headers{
          "Content-Type" =>  "application/merge-patch+json",
        }

        response = raw_patch path, {subresource => args}.to_json, headers: headers
        parse_response(response)
      end

      def delete_{{singular_method_name.id}}(resource : {{type}})
        delete_{{singular_method_name.id}} name: resource.metadata.name, namespace: resource.metadata.namespace
      end

      def delete_{{singular_method_name.id}}(name : String{% if cluster_wide == false %}, namespace : String = "default"{% end %}, *, propagation_policy : PropagationPolicy = :background)
        params = URI::Params{"propagationPolicy" => propagation_policy.to_s}
        path = "/{{prefix.id}}/{{group.id}}/{{version.id}}{% if cluster_wide == false %}/namespaces/#{namespace}{% end %}/{{name.id}}/#{name}?#{params}"
        response = delete path
        JSON.parse response.body
      end

      def watch_{{plural_method_name.id}}(resource_version = "0", timeout : Time::Span = 10.minutes, namespace : String? = nil, labels label_selector : String = "")
        params = URI::Params{
          "watch" => "1",
          "timeoutSeconds" => timeout.total_seconds.to_i64.to_s,
          "labelSelector" => label_selector,
        }
        if namespace
          namespace = "/namespaces/#{namespace}"
        end
        get_response = nil
        loop do
          params["resourceVersion"] = resource_version

          return get "/{{prefix.id}}/{{group.id}}/{{version.id}}#{namespace}/{{name.id}}?#{params}" do |response|
            get_response = response
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
                  resource_version = match[1]
                end
                # If this is an error of some kind, we don't care we'll just run
                # another request starting from the last resource version we've
                # worked with.
                next
              end

              watch = Watch({{type}}).from_json(json_string)

              # If there's a JSON parsing failure and we loop back around, we'll
              # use this resource version to pick up where we left off.
              if new_version = watch.object.metadata.resource_version.presence
                resource_version = new_version
              end

              yield watch
            end
          end
        rescue ex : IO::EOFError
          # Server closed the connection after the timeout
        rescue ex : IO::Error
          @log.warn { ex }
          sleep 1.second # Don't hammer the server
        rescue ex : JSON::ParseException
          # This happens when the watch request times out. This is expected and
          # not an error, so we just ignore it.
          unless ex.message.try &.includes? "Expected BeginObject but was EOF at line 1, column 1"
            @log.warn { "Cannot parse watched object: #{ex}" }
          end
        end
      ensure
        @log.warn { "Exited watch loop for {{plural_method_name.id}}, response = #{get_response.inspect}" }
      end
    end
    {% debug if flag? :debug_define_resource %}
  end
end
