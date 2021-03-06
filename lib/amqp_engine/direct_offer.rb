require "amqp_engine/version"

class DirectOffer
  include Sneakers::Worker
  from_queue 'api_events'

  def work( msg )
    event = Event.new( msg )
    mongohq_url = ENV['MONGOHQ_URL'] || 'mongodb://127.0.0.1:27017/api_events'
    mongo_client = Moped::Session.connect(  mongohq_url )
    mongo_client.use('api_events') unless ENV['MONGOHQ_URL']
    mongo_client.with( safe: true ) do | session |
      session[ event.mongo_db ].insert event.adapt
    end
    ack!
  end
end
