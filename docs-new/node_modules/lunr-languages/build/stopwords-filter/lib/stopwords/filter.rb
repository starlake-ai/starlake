module Stopwords

  class Filter

    attr_reader :stopwords

    def initialize stopwords
      @stopwords = stopwords
    end

    def filter words
      words - @stopwords
    end

    def stopword? word
      stopwords.include? word
    end

  end

end
