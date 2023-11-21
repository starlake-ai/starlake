module Stopwords
  module Snowball
    class Stopwords::Snowball::WordSieve
      def initialize custom_list = []
        @filters = Dir[File.dirname(__FILE__) + '/locales/*.csv'].each_with_object({}) do |file, filters|
          lang = File.basename(file, '.csv').to_sym
          filters[lang] = Stopwords::Snowball::Filter.new lang, custom_list
        end
      end

      def stopword? args={}
        args[:lang] ? @filters[args[:lang]].stopword?(args[:word] ) : false
      end

      def filter args={}
        args[:lang] ? @filters[args[:lang]].filter(args[:words] ) : args[:words]
      end
    end
  end
end
