require 'amqp_engine/version'
require 'newrelic_rpm'
require 'new_relic/agent/method_tracer'

class PgImport
  include Sneakers::Worker
  include ::NewRelic::Agent::Instrumentation::ControllerInstrumentation
  include ::NewRelic::Agent::MethodTracer

  from_queue 'regular_tasks',
    prefetch: 1,
    threads: 1

  def work( msg )
    NewRelic::Agent.set_transaction_name("custom/pg_import")
    start = Time.new

    env = ENV['RACK_ENV'] || 'development'

    if env == 'production' or env == 'staging'
      ActiveRecord::Base.establish_connection( "#{ENV[ 'DATABASE_URL' ]}?pool=#{ENV[ 'CONNECTION_POOL' ]}" )
    else
      configuration = YAML::load(IO.read('config/database.yml'))
      ActiveRecord::Base.establish_connection( configuration[ env ] )
    end

    mongohq_url = ENV['MONGOHQ_URL'] || 'mongodb://127.0.0.1:27017/api_events'
    mongo_client = Moped::Session.connect(  mongohq_url )

    collections = collections_for( msg )
    collections.each do | collection, params |
      events = mongo_client[ collection ].find( triggered_at: { "$lt" => start.to_i }, imported: false )

      items = events.map do |json_params| 
        clean_params = json_params
        clean_params.delete( '_id' )
        clean_params.delete( 'imported' )
        params[ :klass ].new clean_params
      end
      begin
        params[ :klass ].import items
      rescue
        NewRelic::Agent.set_transaction_name('pg_import/duplicated_id')
      end
      mongo_client[ collection ].find( triggered_at: { "$lt" => start.to_i }, imported: false ).update_all( '$set' => { imported: true } )

      for_dealer_stats = mongo_client[ collection ].aggregate( [ [ { '$match' => { triggered_at: { "$lt" => start.to_i } } }],
                                                                 [ { '$group' => {'_id' => '$from', 'summa' => { '$sum' => 1 } } } ] ])


      for_dealer_stats.each do | event |
        params = RedirectCode.decode( event[ '_id' ] )
        amount = event['summa']
        DealerStat.trigger( collection, dealer: params[ :dealer_id ], offer: params[ :offer_id ], amount: amount )
        publish( "update_dealer_id_#{ params[ :dealer_id ] }", :to_queue => 'cash_update')
      end
      mongo_client[ collection ].find( triggered_at: { "$lt" => start.to_i } ).remove_all
    end

    ack!
  end

  def collections_for( msg )
    case msg
    when 'import_direct_offers'
      { 
        'direct_offer_redirect': { klass: DirectOfferRedirect }
      }
    end
  end

  def congruence_class
    Object.const_get( "DirectOffer#{ @congruence_word }" )
  end

  add_transaction_tracer :work, :category => :task
  add_method_tracer :work, 'Custom/pg_import'
end
