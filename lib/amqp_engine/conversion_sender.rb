require "amqp_engine/version"

class ConversionSender
  include Sneakers::Worker
  from_queue 'covnersion_sender'

  def work( msg )

    mongohq_url = ENV['MONGOHQ_URL'] || 'mongodb://127.0.0.1:27017/api_events'
    mongo_client = Moped::Session.connect(  mongohq_url )

    case msg
    id = when 'mongo_offer_set_show'
      mongo_client[ 'mongo_event_atomics' ].find( event_name: 'clicker', dealer_id: 7 ).first
    when 'pg_offer_set_show'
      OfferSetShow.not_converted.where( dealer_id: 7 ).first
    when 'pg_direct_offer'
      DirectOfferRedirect.not_converted.where( dealer_id: 7 ).first
    end
    conversion_url = "http://api.richpays.com/apps_advert/glispa/aff_sub=#{ id }"
    RestClient.get conversion_url
    ack!
  end
end
