require 'bson'
require 'mongo'
require 'log4r'
require 'em-http'
require 'yajl'
require 'eventmachine'
require 'byebug'

include EM

@log = Log4r::Logger.new('github')
@log.add(Log4r::StdoutOutputter.new('console', {
  :formatter => Log4r::PatternFormatter.new(:pattern => "[#{Process.pid}:%l] %d :: %m")
}))

EM.run do
  byebug
  last_id = 0 #TODO: input from main crawler
  client = Mongo::Client.new([ '127.0.0.1:27017' ], :database => 'test')
  collection = client[:github]

  stop = Proc.new do
    puts "Terminating crawler"
    EM.stop
  end

  Signal.trap("INT",  &stop)
  Signal.trap("TERM", &stop)

  @data = []
  @usernames = lambda { |e| { username: e['login'] } if e['type'] == 'User' }

  process = Proc.new do
    req = HttpRequest.new("https://api.github.com/users?since=#{last_id}", {
      :inactivity_timeout => 5,
      :connect_timeout => 5
    }).get({
      :head => {
        'Authorization' => "token b892619f81c44683aa5017fe087469036e32997e"##{ENV['GITHUB_TOKEN']}"
      }
    })

    req.callback do
      begin
        data = Yajl::Parser.parse(req.response)
        docs = data.collect(&@usernames).compact

        result = collection.insert_many(docs)
        last_id = data.last['id']
        byebug
      rescue Exception => e
        @log.error "Processing exception: #{e}, #{e.backtrace.first(5)}"
        @log.error "Response: #{req.response_header}, #{req.response}"
      ensure
        EM.next_tick(&process)
      end
    end

    req.errback do
      @log.error "Error: #{req.response_header.status}, \
                  header: #{req.response_header}, \
                  response: #{req.response}"

      EM.add_timer(3700, &process)
    end
  end

  process.call
end
