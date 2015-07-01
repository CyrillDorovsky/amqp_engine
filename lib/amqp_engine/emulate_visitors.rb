require "amqp_engine/version"

class EmulateVisitors
  include Sneakers::Worker
  from_queue 'emulate_visitors'

  def work( msg )

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
                    'http://api9.dev/s/Dzuo'
                  when 'offer_set_visitor'
                    'http://api9.dev/o/2'
                  when 'direct_offer_visitor'
                    'http://api_rich.dev/6dux'
                  end
    RestClient.get visitor_url, user_agent: 'Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53'
    ack!
  end
end
