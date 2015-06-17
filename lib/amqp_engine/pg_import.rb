require 'amqp_engine/version'


class PgImport
  include Sneakers::Worker
  from_queue 'regular_tasks'

  def work( msg )
    start = Time.new

    env = ENV['RACK_ENV'] || 'development'

    configuration = YAML::load(IO.read('config/database.yml'))
    ActiveRecord::Base.establish_connection( configuration[ env ] )

    mongo_url = mongohq_url = ENV['MONGOHQ_URL'] || [ '127.0.0.1:27017' ]
    mongo_client = Moped::Session.new( mongohq_url )
    mongo_client.use( 'api_events' )

    collections = collections_for( msg )
    collections.each do | collection, params |
      items = mongo_client[ collection ].find( triggered_at: { "$lt" => start.to_i } ).map do |json_params| 
        clean_params = json_params
        clean_params.delete( '_id' )
        params[ :klass ].new clean_params
      end
      params[ :klass ].import items
      mongo_client[ collection ].find( triggered_at: { "$lt" => start.to_i } ).remove_all
    end

    ack!
  end

  def collections_for( msg )
    case msg
    when 'import_direct_offers'
      { 
        'direct_offer_show':     { klass: DirectOfferShow },
        'direct_offer_redirect': { klass: DirectOfferRedirect }
      }
    end
  end

  def congruence_class
    Object.const_get( "DirectOffer#{ @congruence_word }" )
  end

end
