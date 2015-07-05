require "amqp_engine/version"
require 'newrelic_rpm'
require 'new_relic/agent/method_tracer'

class ConversionSender
  include Sneakers::Worker
  include ::NewRelic::Agent::Instrumentation::ControllerInstrumentation
  include ::NewRelic::Agent::MethodTracer
  from_queue 'conversion_sender'

  def work( msg )
    NewRelic::Agent.set_transaction_name("custom/conversion_sender")
    mongohq_url = ENV['MONGOHQ_URL'] || 'mongodb://127.0.0.1:27017/api_events'
    mongo_client = Moped::Session.connect(  mongohq_url )

    env = ENV['RACK_ENV'] || 'development'

    if env == 'production'
      ActiveRecord::Base.establish_connection( "#{ENV[ 'DATABASE_URL' ]}?pool=#{ENV[ 'CONNECTION_POOL' ]}" )
    else
      configuration = YAML::load(IO.read('config/database.yml'))
      ActiveRecord::Base.establish_connection( configuration[ env ] )
    end
    begin
      id = case msg
        when 'mongo_offer_set_show'
          mongo_client[ 'mongo_event_atomics' ].where( event_name: 'offer_set', dealer_id: 1 ).first['_id'].to_s
        when 'pg_offer_set_show'
          OfferSetShow.not_converted.where( dealer_id: 1 ).first.mongo_id.to_s
        when 'pg_direct_offer'
          DirectOfferRedirect.not_converted.where( dealer_id: 1 ).first.request_id
        end
    rescue => e
      puts e
    end
    conversion_url = "http://api.richpays.com/apps_advert/mobvistadirect?placement=#{ id }"
    RestClient.get conversion_url if id
    ack!
  end

  add_transaction_tracer :work, :category => :task
  add_method_tracer :work, 'Custom/conversion_sender'

end
