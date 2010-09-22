$LOAD_PATH << File.join(File.dirname(__FILE__), '..', 'lib')

require "rubygems"
require "resque"
require "resque/redis_composite"

describe Resque::RedisComposite do
  describe "initialize" do
    it "parses redis server as string" do
      Resque.redis = Resque::RedisComposite.new("localhost:5380")
      Resque.redis.client.port.should == 5380
    end
  
    it "understands servers as hash of strings" do
      Resque.redis = Resque::RedisComposite.new("default" => "localhost:5380", "stuff" => "localhost:5381")
      Resque.redis.client.port.should == 5380
      Resque.redis.client("stuff").port.should == 5381
    end
  
    it "explodes if there is no default provided" do
      lambda {
        Resque::RedisComposite.new("foo" => "localhost:5380", "bar" => "localhost:5381")
      }.should raise_error
    end    
  end
  
  describe "redis interface" do
    before do
      @default_server = mock("default")      
      @special_server = mock("special")
    end
  
    it "pushes jobs onto the specific Redis server" do
      @special_server.should_receive(:sadd)
      @special_server.should_receive(:rpush)

      Resque.redis = Resque::RedisComposite.new("default" => @default_server, "stuff" => @special_server)
      Resque.push("stuff", :class => "SomeClass", :args => "do something")
    end

    it "pushes jobs onto the default Redis server" do
      @default_server.should_receive(:sadd)
      @default_server.should_receive(:rpush)
    
      @special_server = mock("special")

      Resque.redis = Resque::RedisComposite.new("default" => @default_server, "stuff" => @special_server)
      Resque.push("email", :class => "SomeOtherClass", :args => "do something else")
    end

    it "pops jobs off the specific Redis server" do
      @special_server.should_receive(:lpop)

      Resque.redis = Resque::RedisComposite.new("default" => @default_server, "stuff" => @special_server)
      Resque.pop("stuff")
    end

    it "pops jobs off the default Redis server" do
      @default_server.should_receive(:lpop)

      Resque.redis = Resque::RedisComposite.new("default" => @default_server, "stuff" => @special_server)
      Resque.pop("email")
    end
  end
end