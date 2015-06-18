#require 'amqp_engine/models/direct_offer_show'
#require 'amqp_engine/models/direct_offer_redirect'
class Event

  LIST = %w( direct_offer_redirect direct_offer_show )

  attr_accessor :message, :mongo_db, :congruence_word, :congruence, :collection_name

  def initialize( event_json )
    @message = pull_data( event_json )
    if @message
      @mongo_db = @message[ 'event' ]
    end
    @congruence_word = @mongo_db ? ( LIST & [ @mongo_db ] ).first : nil
#    @congruence = @fact ? congruence_class.new( adapt ) : nil
  end

  def pull_data( event_json )
    JSON.parse( event_json )
  end

  def congruence_class
    Object.const_get( "DirectOffer#{ @congruence_word }" )
  end

  def load_to_mongo
  end

  def adapt
    if @congruence_word == 'direct_offer_show'
      { 
        "request_id"      => @message['request']['request_id'],
        "referer"         => @message['request']['referrer'], 
        "sub_id"          => @message['params']['sub_id'], 
        "sub_id2"         => @message['params']['sub_id2'], 
        "sub_id3"         => @message['params']['sub_id3'], 
        "seller_url"      => @message['offer']['seller_url'], 
        "url_params"      => @message['rack.url_scheme'], 
        "dealer_id"       => @message['offer']['dealer_id'], 
        "offer_id"        => @message['offer']['offer_id'], 
        "direct_offer_id" => nil, 
        "country_id"      => nil, 
        "country_name"    => nil, 
        "country_code"    => @message['request']['country'], 
        "ip"              => @message['request']['ip'], 
        "user_agent"      => @message['request']['user_agent'],
        "user_platform"   => @message['request']['user_platform'], 
        "user_device"     => @message['request']['user_platform'],
        "timestamp"       => @message['request']['timestamp'], 
        "uniq_visitor"    => @message['params']['uniq'], 
        "converted"       => false, 
        "triggered_at"    => Time.new.to_i
      }
    elsif @congruence_word == 'direct_offer_redirect'
      { 
        "request_id"      => @message['request']['request_id'],
        "referer"         => @message['request']['referrer'], 
        "sub_id"          => @message['params']['sub_id'], 
        "sub_id2"         => @message['params']['sub_id2'], 
        "sub_id3"         => @message['params']['sub_id3'], 
        "seller_url"      => @message['offer']['seller_url'], 
        "url_params"      => @message['rack.url_scheme'], 
        "dealer_id"       => @message['offer']['dealer_id'], 
        "offer_id"        => @message['offer']['offer_id'], 
        "direct_offer_id" => nil, 
        "country_id"      => nil, 
        "country_name"    => nil, 
        "country_code"    => @message['request']['country'], 
        "ip"              => @message['request']['ip'], 
        "user_agent"      => @message['request']['user_agent'],
        "user_platform"   => @message['request']['user_platform'], 
        "user_device"     => @message['request']['user_platform'],
        "timestamp"       => @message['request']['timestamp'], 
        "uniq_visitor"    => @message['params']['uniq'], 
        "redirect_url"    => @message['redirect_url'],
        "converted"       => false, 
        "triggered_at"    => Time.new.to_i
      }
    end
  end

end
