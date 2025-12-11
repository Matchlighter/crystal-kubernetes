module Kubernetes
  class SyncedStore(T)
    def initialize(@resource_watcher : ResourceWatcher(T))
      @mutex = Mutex.new
      @cache = Hash(String, T).new
    end

    def self.new(k8s_client : Client, api_path : String)
      new(ResourceWatcher(T).new(k8s_client, api_path))
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

    def disconnect
      @resource_watcher.abort!
    end

    def spawn_watch
      spawn do
        @resource_watcher.start_watch! do |watch|
          obj = watch.object
          key = obj.metadata.namespace.presence ? "#{obj.metadata.namespace}/#{obj.metadata.name}" : obj.metadata.name

          @mutex.synchronize do
            case watch
            when .added?, .modified?
              @cache[key] = obj
              Log.debug { "Updated #{@resource_watcher.api_path} cache for #{key}" }
            when .deleted?
              @cache.delete(key)
              Log.debug { "Removed #{@resource_watcher.api_path} from cache: #{key}" }
            end
          end
        end
      end
    end
  end
end
