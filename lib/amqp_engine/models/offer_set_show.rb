class OfferSetShow < ActiveRecord::Base
  
  belongs_to :offer
  belongs_to :dealer
  belongs_to :offer_set
  belongs_to :campaign
  belongs_to :country

  scope :not_converted, -> { where( converted: false ) }

end
