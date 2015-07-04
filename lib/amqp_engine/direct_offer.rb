require "amqp_engine/version"
require 'newrelic_rpm'
require 'new_relic/agent/method_tracer'

class DirectOffer
  include Sneakers::Worker
  include ::NewRelic::Agent::Instrumentation::ControllerInstrumentation
  include ::NewRelic::Agent::MethodTracer

  from_queue 'api_events'

  def work( msg )
    NewRelic::Agent.set_transaction_name("custom/direct_offer")
    event = Event.new( msg )
    mongohq_url = ENV['MONGOHQ_URL'] || 'mongodb://127.0.0.1:27017/api_events'
    mongo_client = Moped::Session.connect(  mongohq_url )
    mongo_client.use('api_events') unless ENV['MONGOHQ_URL']
    mongo_client.with( safe: true ) do | session |
      session[ event.mongo_db ].insert event.adapt
    end
    ack!
  end

  add_transaction_tracer :work, :category => :task
  add_method_tracer :work, 'Custom/direct_offer'
end
