module Stopwords
  module Snowball
    class Filter < Stopwords::Filter
      attr_reader :locale
      attr_reader :locale_filename

      def initialize locale, custom_list = []
        @locale = locale
        @locale_filename = "#{File.dirname(__FILE__)}/locales/#{locale}.csv"

        raise "Unknown locale" unless File.exists?(@locale_filename)
        super File.read(@locale_filename).split(",") + custom_list 
      end
    end
  end
end
