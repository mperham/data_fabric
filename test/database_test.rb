require 'test_helper'
require 'flexmock/test_unit'
require 'erb'

class TheWholeBurrito < ActiveRecord::Base
  data_fabric :prefix => 'fiveruns', :replicated => true, :shard_by => :city
end

class DatabaseTest < Test::Unit::TestCase
  
  def setup
    ActiveRecord::Base.configurations = load_database_yml
    DataFabric::ConnectionProxy.shard_pools.clear
  end

  def test_features
    DataFabric.activate_shard :city => :dallas do
      assert_equal 'fiveruns_city_dallas_test_slave', TheWholeBurrito.connection.connection_name
      assert_equal DataFabric::PoolProxy, TheWholeBurrito.connection_pool.class
      assert !TheWholeBurrito.connected?

      # Should use the slave
      burrito = TheWholeBurrito.find(1)
      assert_match 'vr_dallas_slave', burrito.name

      assert TheWholeBurrito.connected?
    end
  end

  def test_live_burrito
    DataFabric.activate_shard :city => :dallas do
      assert_equal 'fiveruns_city_dallas_test_slave', TheWholeBurrito.connection.connection_name

      # Should use the slave
      burrito = TheWholeBurrito.find(1)
      assert_match 'vr_dallas_slave', burrito.name

      # Should use the master
      burrito.reload
      assert_match 'vr_dallas_master', burrito.name

      # ...but immediately set it back to default to the slave
      assert_equal 'fiveruns_city_dallas_test_slave', TheWholeBurrito.connection.connection_name

      # Should use the master
      TheWholeBurrito.transaction do
        burrito = TheWholeBurrito.find(1)
        assert_match 'vr_dallas_master', burrito.name
        burrito.name = 'foo'
        burrito.save!
      end
    end
  end
  
  def test_we_reuse_connections
    [:austin, :dallas].each do |city|
      DataFabric.activate_shard :city => city do
        assert_equal "fiveruns_city_#{city}_test_slave", TheWholeBurrito.connection.connection_name, "city: #{city}"
        assert_equal DataFabric::PoolProxy, TheWholeBurrito.connection_pool.class, "city: #{city}"
        assert !TheWholeBurrito.connected?, "city: #{city}"
   
        # Should use the slave
        burrito = TheWholeBurrito.find(1)
        assert_match "vr_#{city}_slave", burrito.name, "city: #{city}"
   
        assert TheWholeBurrito.connected?, "city: #{city}"
      end
    end
    
    # Since the burb uses the same db as austin we should re-use that connection instead of creating a new one
    city = :austin_burb
    DataFabric.activate_shard :city => city do
      assert_equal "fiveruns_city_#{city}_test_slave", TheWholeBurrito.connection.connection_name, "city: #{city}"
      assert_equal DataFabric::PoolProxy, TheWholeBurrito.connection_pool.class, "city: #{city}"
      assert TheWholeBurrito.connected?, "city: #{city}"
 
      # Should use the slave
      burrito = TheWholeBurrito.find(1)
      assert_match "vr_austin_slave", burrito.name, "city: #{city}"
 
      assert TheWholeBurrito.connected?, "city: #{city}"
    end
    
    db_connections = []
    [:austin, :dallas, :austin_burb].each do |city|
      DataFabric.activate_shard(:city => city) do
        db_connections << TheWholeBurrito.connection.connection
      end
    end
    db_connections = db_connections.uniq
    
    # We should only have 2 db connections since there are 2 uniq dbs
    assert_equal 2, db_connections.size
  end
end
