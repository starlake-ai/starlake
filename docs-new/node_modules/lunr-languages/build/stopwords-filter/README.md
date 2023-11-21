Stopwords Filter
================

This project is a very simple and naive implementation of a stopwords filter that remove a list of banned words (stopwords) from a sentence.

Quick guide
-----------

* Install

just type

```
gem install stopwords-filter
```

or

```
gem 'stopwords-filter'
```

in your Gemfile.

* Use it

  1. Simple version

```
stopwords = ['by', 'written', 'from']
filter = Stopwords::Filter.new stopwords

filter.filter 'guide by douglas adams'.split
# ['guide', 'douglas', 'adams']

filter.stopword? 'by'
# true
```

  2. Snowball version


```
filter = Stopwords::Snowball::Filter.new "en"
filter.filter 'guide by douglas adams'.split
# ['guide', 'douglas', 'adams']

filter.stopword? 'by'
# true
```

  2.1 Snowball version with Sieve class (thanks to @s2gatev)

```ruby
sieve = Stopwords::Snowball::WordSieve.new

filtered = sieve.filter lang: :en, words: 'guide by douglas adams'.split
# filtered = ['guide', 'douglas', 'adams']

sieve.stopword? lang: :en, word: 'by'
# true
```



What is a Stopword?
-------------------

According to [Wikipedia][wikipedia_stopwords]

> In computing, stop words are words which are filtered out prior to, or after, processing of natural language data (text).

And that's it. Words that are removed before you perform some task on the rest of them.

Why would I want to remove anything?
------------------------------------

Imagine you have a database of products and you want your customers to search on them. You can't use a proper search engine (such as [Solr][solr], [Sphinx][sphinx] or even [Google][google]) neither full search systems from popular database systems such as [PostgreSQL][postgre]. You are left alone with LIKEs and %.

You have your fake search engine working. Someone searches 'Guide Douglas Adams' and you find 'Douglas Adams - Hitchhiker's guide to the galaxy' everything is perfect.

But then someone searches 'guide by douglas adams' and you don't find anything. You don't have any 'by' in the description or title of the book! Most importantly, you don't need that 'by'!

You wish you could get rid of all those 'by' or 'written' or 'from', huh? That's why we are here!

How this thing works?
---------------------

Main class of this 'library' is Stopwords::Filter You just create a new object with an array of stopwords

```
stopwords = ['by', 'written', 'from']
filter = Stopwords::Filter.new stopwords
```

And then you have it, you just can filter

```
filter.filter 'guide by douglas adams'.split  #-> ['guide', 'douglas', 'adams']
```

That's all?
-----------

I know what you're thinking, it takes a line of ruby code to filter one array from other. That's why we have added an extra functionality, [Snowball][wikipedia_snowball] stopwords lists, already built for you and ready to use.

How do I use that snowball thing?
---------------------------------

You just create the filter with the locale you want to use

```
filter = Stopwords::Snowball::Filter.new "en"
```

And then you filter without worrying about the exact stopwords used

```
filter.filter 'guide by douglas adams'.split  #-> ['guide', 'douglas', 'adams']
```

Anything else?
--------------

In a future version I would like to include a chaining filter where you include a series of operations and they are executed in a lineal order, just like the [Pipes and Filters design pattern][wikipedia_pipes_filters]

Ackonowledgments
----------------

Thanks to @s2gatev who added the `stopword?` method and the sieve class to this gem

  [wikipedia_stopwords]: http://en.wikipedia.org/wiki/Stopword
  [solr]: https://github.com/sunspot/sunspot
  [sphinx]: https://github.com/freelancing-god/thinking-sphinx
  [google]: https://github.com/alexreisner/google_custom_search
  [postgre]: https://github.com/Casecommons/pg_search
  [wikipedia_snowball]: http://en.wikipedia.org/wiki/Snowball_programming_language
  [wikipedia_pipes_filters]: http://en.wikipedia.org/wiki/Pipes_and_filters

