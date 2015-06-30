class DirectOfferRedirect < ActiveRecord::Base

  belongs_to :offer
  belongs_to :dealer
  belongs_to :offer_set
  belongs_to :campaign
  belongs_to :country
  has_one    :dealer_increment, as: :reason

  scope :not_converted, -> { where( converted: false ) }

  def self.params_for_conversion( direct_offer_redirect )
    { sub_id:        direct_offer_redirect.sub_id,
      sub_id2:       direct_offer_redirect.sub_id2,
      sub_id3:       direct_offer_redirect.sub_id3,
      revenue:       direct_offer_redirect.dealer.dealer_revenue( direct_offer_redirect.offer ),
      referer:       direct_offer_redirect.referer,
      country_id:    direct_offer_redirect.country_id,
      country_name:  direct_offer_redirect.country_name,
      country_code:  direct_offer_redirect.country_code,
      ip:            direct_offer_redirect.ip.to_s,
      user_agent:    direct_offer_redirect.user_agent,
      user_platform: direct_offer_redirect.user_platform,
      uniq:          direct_offer_redirect.uniq_visitor,
      direct_offer_redirect_id: direct_offer_redirect.id }
  end

  def set_converted
    update! converted: true
  end

end
