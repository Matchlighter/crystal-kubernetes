module Kubernetes
  class SyncedStore(T)
    def initialize(@k8s_client : Client, @api_path : String)
      @mutex = Mutex.new
      @cache = Hash(String, T).new
    end

    def get(namespace : String, name : String) : T?
      get "#{namespace}/#{name}"
    end

    def get(name : String) : T?
      @mutex.synchronize do
        @cache[name]
      end
    end

    def all : Array(T)
      @mutex.synchronize do
        @cache.values
      end
    end

    def all_for(namespace : String) : Array(T)
      @mutex.synchronize do
        @cache.values.select { |obj| obj.metadata.namespace == namespace }
      end
    end

    def all_for(namespace : Kubernetes::Namespace) : Array(T)
      all_for(namespace.metadata.name.not_nil!)
    end

    def [](name : String) : T?
      get(name)
    end

    def spawn_watch
      spawn do
        @k8s_client.watch_resource(T, @api_path) do |watch|
          obj = watch.object
          key = obj.metadata.namespace.presence ? "#{obj.metadata.namespace}/#{obj.metadata.name}" : obj.metadata.name

          @mutex.synchronize do
            case watch
            when .added?, .modified?
              @cache[key] = obj
              Log.debug { "Updated #{@api_path} cache for #{key}" }
            when .deleted?
              @cache.delete(key)
              Log.debug { "Removed #{@api_path} from cache: #{key}" }
            end
          end
        end
      end
    end
  end
end
