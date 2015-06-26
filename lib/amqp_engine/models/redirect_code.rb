module RedirectCode

  REDIRECT_CODE_SALT = 'richpays number one'

  class << self

    def generate( dealer, offer )
      hashids = Hashids.new( REDIRECT_CODE_SALT )
      hashids.encode( [ dealer.id, offer.id ] )
    end

    def decode( hashid )
      hashids = Hashids.new( REDIRECT_CODE_SALT )
      begin
        values = hashids.decode( hashid )
        { dealer_id: values[ 0 ], offer_id: values[ 1 ] }
      rescue Hashids::InputError => e
        { dealer_id: nil, offer_id: nil }
      end
    end  

  end

end
