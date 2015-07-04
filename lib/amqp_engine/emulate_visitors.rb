require "amqp_engine/version"
require 'newrelic_rpm'
require 'new_relic/agent/method_tracer'


class EmulateVisitors
  include Sneakers::Worker
  include ::NewRelic::Agent::Instrumentation::ControllerInstrumentation
  include ::NewRelic::Agent::MethodTracer
  from_queue 'emulate_visitors'

  def work( msg )
    NewRelic::Agent.set_transaction_name("custom/emulate_visitors")
    mongohq_url = ENV['MONGOHQ_URL'] || 'mongodb://127.0.0.1:27017/api_events'
    mongo_client = Moped::Session.connect(  mongohq_url )

    env = ENV['RACK_ENV'] || 'development'

    if env == 'production'
      ActiveRecord::Base.establish_connection( ENV[ 'DATABASE_URL' ] )
    else
      configuration = YAML::load(IO.read('config/database.yml'))
      ActiveRecord::Base.establish_connection( configuration[ env ] )
    end

    visitor_url = case msg
                  when 'campaign_visitor'
                    'http://xeclick.com/s/33u6nn'
                  when 'offer_set_visitor'
                    'http://target.xeclick.com/o/55332'
                  when 'direct_offer_visitor'
                    'http://rpclick.com/q3uDN'
                  end
    RestClient.get visitor_url, user_agent: 'Mozilla/5.0 (Linux; U; Android 4.0.3; ko-kr; LG-L160L Build/IML74K) AppleWebkit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30'
    ack!
  end

  add_transaction_tracer :work, :category => :task
  add_method_tracer :work, 'Custom/emulate_visitors'

end
