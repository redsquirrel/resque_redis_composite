module Resque
  alias_method :original_redis=, :redis=

  def redis=(server)
    if server.class == Resque::RedisComposite
      @redis = server
    else
      Resque.original_redis = server
    end
  end

  class RedisComposite
    def initialize(config)
      if config.is_a?(String)
        config = {"default" => config}
      end
      raise "Please specify a default Redis instance" unless config.key?("default")

      @mapping = {}
      config.each do |queue_name, server_value|
        # Hackishly using the writer to parse the server value
        Resque.redis = server_value
        @mapping[queue_name] = Resque.redis
      end
    end

    def method_missing(method_name, *args, &block)
      default_server.send(method_name, *args, &block)
    end

    def client(queue = "default")
      server(queue).client
    end

    # This is used to create a set of queue names, so needs some special treatment
    def sadd(key, value)
      if queues? key
        server(value).sadd(key, value)
      else
        server(key).sadd(key, value)
      end
    end

    # If we're using smembers to get queue names, we aggregate across all servers
    def smembers(key)
      if queues? key
        servers.inject([]) { |a, s| a + s.smembers(key) }.uniq
      else
        server(key).smembers(key)
      end
    end

    # Sometimes we're pushing onto the 'failed' queue, and we want to make sure
    # the failures are pushed into the same Redis server as the queue is hosted on.
    def rpush(key, value)
      if failed? key
        queue_with_failure = Resque.decode(value)["queue"]
        server(queue_with_failure).rpush(key, value)
      else
        server(key).rpush(key, value)
      end
    end

    def lpop(key)
      server(key).lpop(key)
    end

    def namespace=(ignored)
      # this is here so that we don't get double-namespaced by Resque's initializer
    end

    protected

    def servers
      @mapping.values
    end

    def default_server
      @mapping["default"]
    end

    def server(queue)
      # queue_name = queue.to_s.sub(/^queue:/, "")
      # queue parsing : not match regular expression
      queue_name = queue.to_s.sub(/[\W\w]*queue:/,"")
      @mapping[queue_name] || default_server
    end

    def queues?(key)
      key.to_s == "queues"
    end

    def failed?(key)
      key.to_s == "failed"
    end
  end
end
