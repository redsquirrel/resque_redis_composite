module Resque
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
    
    def default_server
      @mapping["default"]
    end
    
    def server(queue)
      queue_name = queue.sub(/^resque:queue:/, "")
      @mapping[queue_name] || default_server
    end
    
    def client(queue = "default")
      server(queue).client
    end
    
    # This is used to create a queue of queue names, so needs some special treatment
    def sadd(key, value)
      if key == "resque:queues"
        server(value).sadd(key, value)
      else
        server(key).sadd(key, value)
      end
    end
    
    def rpush(key, value)
      server(key).rpush(key, value)
    end
    
    def lpop(key)
      server(key).lpop(key)
    end
  end
end