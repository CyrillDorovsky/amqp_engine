class DealerStat < ActiveRecord::Base

  belongs_to :dealer
  belongs_to :campaign

  scope :hourly,  -> { where( hourly:  true ) }
  scope :daily,   -> { where( daily:   true ) }
  scope :monthly, -> { where( monthly: true ) }

  scope :common,      -> { where( common:  true ) }
  scope :wapclick,    -> { where( wapclick:  true ) }
  scope :apps_advert, -> { where( apps_advert:  true ) }
  scope :cpa,         -> { where( cpa:  true ) }

  scope :no_grouping,       -> { where( no_grouping: true ) }
  scope :campaign_grouping, -> { where( campaign_grouping: true ) }
  scope :offer_grouping,    -> { where( offer_grouping: true ) }

  scope :triggered_at_asc, -> { order( 'triggered_at ASC' ) }

  scope :today,           -> { where( triggered_at: Time.zone.now.beginning_of_day..Time.zone.now, hourly: true ) }
  scope :yesterday,       -> { where( triggered_at: 1.day.ago.beginning_of_day..1.day.ago.end_of_day, hourly: true ) }
  scope :last_seven_days, -> { where( triggered_at: 7.day.ago.beginning_of_day..Time.zone.now, daily: true ) }
  scope :current_month,   -> { where( triggered_at: Time.zone.now.beginning_of_month..Time.zone.now, daily: true ) }
  scope :previous_month,  -> { where( triggered_at: 1.month.ago.beginning_of_month..1.month.ago.end_of_month, daily: true ) }
  scope :all_time,        -> { where( monthly: true ) }
  scope :last_month,      -> { where( created_at: 1.month.ago..Time.zone.now ) }

  validates :md5id, presence: true, uniqueness: true
  #after_create :check_hour, if: Proc.new { hourly == true }


  WAPCLICK_EVENTS    = %i( subscribe unsubscribe rebill buyout )
  APPS_ADVERT_EVENTS = %i( app_install )
  COMMON_EVENTS      = %i( visitor uniq_visitor uniq_traf_back traf_back )

  def check_hour
    if hour_is_invalid
      current_hour       = Time.zone.now.hour
      search             = methods_for_md5
      search[ :current ] = current_hour
      search[ :hour ]    = current_hour
      destroy
      stat               = DealerStat.find_or_create_with_md5( search )
    end
  end

  def methods_for_md5
    methods = %w( dealer_id campaign_id offer_id hourly daily monthly month day no_grouping campaign_grouping offer_grouping common wapclick cpa apps_advert )
    context  = Hash.new
    methods.each { | method_name | context[ method_name ] = send method_name }
    context
  end

  def hour_is_invalid
    created_hour = created_at.hour
    current != created_hour or hour != created_hour
  end

  def self.clean_dubles
    where( created_at: 2.hours.ago..Time.now ).map do | st |
      begin
        md5id = st.set_md5
        st.save!
      rescue
        st.check_for_doubles
      end
    end
  end

  def check_for_doubles
    md5id = set_md5
    if valid?
      save!
    else
      original_stat = DealerStat.find_by( md5id: md5id )
      original_stat.visitor              += visitor
      original_stat.uniq_visitor         += uniq_visitor
      original_stat.traf_back            += traf_back
      original_stat.money_total          += money_total
      original_stat.money_waiting        += money_waiting
      original_stat.money_confirmed      += money_confirmed
      original_stat.money_declined       += money_declined
      original_stat.download             += download
      original_stat.uniq_traf_back       += uniq_traf_back
      original_stat.subscribe            += subscribe
      original_stat.rebill               += rebill
      original_stat.unsubscribe          += unsubscribe
      original_stat.conversion_total     += conversion_total
      original_stat.conversion_confirmed += conversion_confirmed
      original_stat.conversion_waiting   += conversion_waiting
      original_stat.conversion_declined  += conversion_declined
      original_stat.conversion_subscribe += conversion_subscribe
      original_stat.conversion_rebill    += conversion_rebill
      original_stat.money_from_rebill    += money_from_rebill
      original_stat.money_from_buyout    += money_from_buyout
      original_stat.conversion_buyout    += conversion_buyout
      original_stat.conversion_install   += conversion_install
      original_stat.js_visitor           += js_visitor
      original_stat.uniq_js_visitor      += uniq_js_visitor
      original_stat.webland_visitor      += webland_visitor
      original_stat.uniq_webland_visitor += uniq_webland_visitor
      original_stat.conversion_app_install += conversion_app_install
      original_stat.conversion_unsubscribe += conversion_unsubscribe
      original_stat.save!
      self.delete
    end
  end

  def self.trigger( fact, context = {} )
    unless context[ :empty ]
      search_for( fact, context ).each do | search |
        stat = find_or_create_with_md5( search )
        stat.increase( increments_for( fact, context ) )
        if search[ :no_grouping ] && context[ :offer ]
          common_stat = find_or_create_with_md5( common_search_options( search ) )
          common_stat.increase( increments_for( fact, context ) )
        end
      end
    end
  end

  def self.trigger_with_amount( fact, context = {} )
    search_for( fact, context ).each do | search |
      stat = find_or_create_with_md5( search )
      stat.increase( increments_for( fact.to_sym, context ) )
      if search[ :no_grouping ] && context[ :offer ]
        common_stat = find_or_create_with_md5( common_search_options( search ) )
        common_stat.increase( increments_for( fact.to_sym, context ) )
      end
    end
  end


  def self.common_search_options( search = {} )
    kind = search[ :purpose ]
    search[ :common ] = true
    search[ :purpose ] = :common
    search[ :no_grouping ] = true
    search.except( :offer_grouping, :offer_id, :campaign_grouping, :campaign_id, kind )
  end

  def set_md5
    methods = %w( dealer_id campaign_id offer_id hourly daily monthly current month day hour no_grouping campaign_grouping offer_grouping common wapclick cpa apps_advert )
    context = methods.map { | ability | [ ability, send( ability ) ] }
    self.md5id = Digest::MD5.hexdigest( context.to_s )
  end

  def self.find_or_create_with_md5( search )
    stat = new( search )
    begin
      md5id = stat.set_md5
      stat.save!
    rescue
      stat = find_by( md5id: md5id )
    end
    stat
  end

  def self.increments_for( fact, context = {} )
    case fact
    when :visitor
      { visitor: context[ :amount ] ? context[ :amount ] : 1 }
    when :uniq_visitor
      { uniq_visitor: context[ :amount ] ? context[ :amount ] : 1, 
        visitor:      context[ :amount ] ? context[ :amount ] : 1 }
    when :js_visitor
      { js_visitor: context[ :amount ] ? context[ :amount ] : 1 }
    when :uniq_js_visitor
      { js_visitor:      context[ :amount ] ? context[ :amount ] : 1, 
        uniq_js_visitor: context[ :amount ] ? context[ :amount ] : 1 }
    when :webland_visitor
      { webland_visitor: 1 }
    when :uniq_webland_visitor
      { webland_visitor:      context[ :amount ] ? context[ :amount ] : 1,
        uniq_webland_visitor: context[ :amount ] ? context[ :amount ] : 1 }
    when :uniq_traf_back
      { uniq_traf_back: context[ :amount ] ? context[ :amount ] : 1 }
    when :traf_back
      { traf_back: context[ :amount ] ? context[ :amount ] : 1 }
    when :subscribe
      { conversion_subscribe: 1 }
    when :unsubscribe
      { conversion_unsubscribe: 1 }
    when :buyout
      { conversion_buyout: 1, money_from_buyout: context[ :offer ].buyout_price, money_total: context[ :offer ].buyout_price }
    when :rebill
      { conversion_rebill: 1, money_from_rebill: dealer_revenue( context ), money_total: dealer_revenue( context ) }
    when :app_install
      if context[ :amount ]
        { conversion_app_install: context[ :amount ], money_total: dealer_revenue( context ) }
      else
        { conversion_app_install: 1, money_total: dealer_revenue( context ) }
      end
    else
      { empty: true }
    end
  end

  def self.dealer_revenue( context = {} )
    if context[ :amount ]
      context[ :campaign ].dealer.dealer_revenue( context[ :offer ] ) * context[ :amount ]
    else
      context[ :campaign ].dealer.dealer_revenue( context[ :offer ] )
    end
  end

  def self.search_for( fact, context = {} )
    basis = {}
    basis[ :dealer_id ]   = context[ :campaign ].dealer_id
    basis[ :purpose ]     = 'apps_advert'
    basis[ :offer_id ]    = context[ :offer ] ? context[ :offer ].id : nil
    basis[ :campaign_id ] = context[ :campaign ].id
    basis[ 'apps_advert' ] = true
    if context[ :current ] 
      basis[ :current_timestamp ] = context[ :current ] 
      context.delete :current
    end
    wrap_search_basis( basis )
  end

  def self.wrap_search_basis( basis )
    time_options = %i( hourly daily monthly ).map { | regularity | add_current_time( regularity, basis ) }
    basis.delete( :current_timestamp ) if basis[ :current_timestamp ]
    basis = time_options.map { | options | options.merge basis }
    result = wrap_basis_with_groupings( basis )
    result.flatten
  end

  def self.wrap_basis_with_groupings( basis )
    basis.map do | options |
      if options[ :purpose ]  == :common
        add_no_grouping( options )
      else
        add_groupings( options )
      end
    end
  end

  def self.add_groupings( options )
    [ { no_grouping:       true }.merge( options.except( :offer_id, :campaign_id ) ),
      { offer_grouping:    true }.merge( options.except( :campaign_id ) ),
      { campaign_grouping: true }.merge( options.except( :offer_id ) ) ]
  end

  def self.add_no_grouping( options )
    [ { no_grouping:       true }.merge( options.except( :offer_id, :campaign_id ) ) ]
  end

  def increase( increments = {} )
    increments.each do | counter_name, amount |
      increment!( counter_name, amount )
    end
  end

  def self.add_current_time( regularity, basis )
    result = {}
    current_time = basis[ :current_timestamp ] ? Time.zone.at( basis[ :current_timestamp] ) : Time.zone.now
    result[ regularity ] = true
    case regularity
    when :hourly
      result[ :hour ] =  current_time.hour
      result[ :day ] =   current_time.day
      result[ :month ] = current_time.month
      result[ :current ] = current_time.send( scope_to_time( regularity ) )
    when :daily
      result[ :day ] =   current_time.day
      result[ :month ] = current_time.month
      result[ :current ] = current_time.send( scope_to_time( regularity ) )
    when :monthly
      result[ :month ] =   current_time.month
      result[ :current ] = current_time.send( scope_to_time( regularity ) )
    end
    result[ :triggered_at ] = current_time
    result
  end

  def self.scope_to_time( regularity )
    result = 'hour'  if regularity == :hourly
    result = 'day'   if regularity == :daily
    result = 'month' if regularity == :monthly
    result
  end

  def self.dashboard_total_money( dealer_id )
    apps_advert_daily   = money_for_dash( dealer_id, :apps_advert, :daily )
    apps_advert_monthly = money_for_dash( dealer_id, :apps_advert, :monthly )

    { 
      "apps_advert_daily"   => apps_advert_daily,
      "apps_advert_monthly" => apps_advert_monthly
    }
  end

  def self.money_for_dash( dealer_id, kind, range )
    options = { dealer_id: dealer_id, no_grouping: true }
    options[ kind ] = true
    options[ range ] = true
    last = where( options ).last
    if last
      if last.monthly
        result = last.current == Time.zone.now.month ? last.money_total : 0
      elsif last.daily
        result = last.current == Time.zone.now.day   ? last.money_total : 0
      end
    else
      result = 0
    end
    result
  end

  def self.records_for_caching( dealer, params={} )
    load_collection( dealer, params.deep_symbolize_keys )
  end

  def self.restricted_caching_params( params={} )
    %i[ date_start date_end ] & params.keys.map(&:to_sym)
  end

  def self.date_params_to_range( params )
    if %w[ day week month quarter year ].include? params[ :date_scope ]
      scope_to_start_date( params[ :date_scope ] )..Time.zone.now.end_of_day
    else
      params[ :date_start ] ||= 1.week.ago.in_time_zone.beginning_of_day.to_s
      params[ :date_end ] ||= Time.zone.now.end_of_day.to_s
      start  = Date.parse( params[ :date_start ] ).in_time_zone.beginning_of_day
      finish = Date.parse( params[ :date_end ] ).in_time_zone.end_of_day
      start..finish
    end
  end

  def self.scope_to_start_date( date_scope )
    {
      day: Time.zone.now.beginning_of_day,
      week: 1.week.ago.in_time_zone.beginning_of_day,
      month: 30.days.ago.in_time_zone.beginning_of_day,
      quarter: 90.days.ago.in_time_zone.beginning_of_day,
      year: 1.year.ago.beginning_of_day
    }[ date_scope.to_sym ]
  end

  def self.load_collection( dealer, params = {} )
    range = date_params_to_range( params )
    grouping     = params[ :grouping ] || :no_grouping
    detalization = get_detalization_from_range( range )
    options = {}
    options[ :apps_advert ] = true
    options[ detalization ] = true
    options[ grouping ]     = true
    options[ :dealer_id ]   = dealer.id
    options[ :triggered_at ]  = range
    if has_grouping( params[ :grouping ] )
      sum_grouping( options )
    else
      where( options ).order( 'triggered_at ASC' )
    end
  end

  def self.has_grouping( grouping )
    %w( offer_grouping campaign_grouping ).include? grouping
  end

  def self.cached_for( dealer_id )
    scopes = %i( today yesterday last_seven_days current_month previous_month all_time )
    raw_pg_data = Hash.new
    scopes.each { | scope | raw_pg_data[ scope ] = load_raw_stats( scope, dealer_id ) }

    format_for_dash( raw_pg_data )
  end

  def self.load_raw_stats( scope, dealer_id )
    send( scope ).common.no_grouping.where( dealer_id: dealer_id ).triggered_at_asc
  end

  def self.format_for_dash( raw_pg_data )
    formatted = Hash.new
    raw_pg_data.each do | scope, stats |
      formatted[ scope ] = formatted_created_and_money( stats )
    end
    formatted
  end

  def self.formatted_created_and_money( stats )
    stats.map do |stat|
      [ ( stat.triggered_at.to_i + 14400 ) * 1000, stat.money_total ]
    end
  end

  def self.get_detalization_from_range( range )
    seconds = ( range.last - range.begin ).abs
    detalization = :daily
    detalization = :hourly  if seconds <= 86400 # 1 сутки
    detalization = :monthly if 7776000 <= seconds && seconds <= 31104000 # 3 месяца..год
    detalization
  end

  def self.load_subscribes( current_dealer )
    { mts:     current_dealer.subscribes.mts.active.count,
      megafon: current_dealer.subscribes.megafon.active.count,
      beeline: current_dealer.subscribes.beeline.active.count }
  end

  def self.sum_grouping( options )
    key = :offer_id    if options[ 'offer_grouping' ] == true
    key = :campaign_id if options[ 'campaign_grouping' ] == true
    DealerStat.select( "#{ key.to_s } as #{ key.to_s },
      BOOL_AND( dealer_stats.wapclick )          as wapclick,
      BOOL_AND( dealer_stats.apps_advert )       as apps_advert,
      BOOL_AND( dealer_stats.cpa )               as cpa,
      BOOL_AND( dealer_stats.common )            as common,
      BOOL_AND( dealer_stats.campaign_grouping ) as campaign_grouping,
      BOOL_AND( dealer_stats.offer_grouping )    as offer_grouping,
      BOOL_AND( dealer_stats.no_grouping )    as no_grouping,
      BOOL_AND( dealer_stats.hourly )            as hourly,
      BOOL_AND( dealer_stats.daily )             as daily,
      BOOL_AND( dealer_stats.monthly )           as monthly,
      SUM( dealer_stats.conversion_total )       as conversion_total,
      SUM( dealer_stats.conversion_declined )    as conversion_declined,
      SUM( dealer_stats.conversion_subscribe )   as conversion_subscribe,
      SUM( dealer_stats.conversion_unsubscribe ) as conversion_unsubscribe,
      SUM( dealer_stats.conversion_rebill )      as conversion_rebill,
      SUM( dealer_stats.conversion_buyout )      as conversion_buyout,
      SUM( dealer_stats.conversion_app_install ) as conversion_app_install,
      SUM( dealer_stats.money_total )            as money_total,
      SUM( dealer_stats.money_declined )         as money_declined,
      SUM( dealer_stats.money_from_rebill )      as money_from_rebill,
      SUM( dealer_stats.money_from_buyout )      as money_from_buyout,
      SUM( dealer_stats.uniq_visitor )           as uniq_visitor,
      SUM( dealer_stats.uniq_js_visitor )        as uniq_js_visitor,
      SUM( dealer_stats.traf_back )              as traf_back").where( options ).group( key )
  end

  def grouping_name
    if attributes.keys.include? 'offer_id'
      name = Offer.find( offer_id ).name if offer_id
    elsif attributes.keys.include? 'campaign_id'
      name = Campaign.find( campaign_id ).name if campaign_id
    end
  end

end
