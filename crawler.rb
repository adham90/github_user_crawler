require 'bson'
require 'mongo'
require 'logger'
require 'em-http'
require 'yajl'
require 'eventmachine'
require 'byebug'

include EM

log = Logger.new('./output.log')
log.formatter = proc do |severity, datetime, _progname, msg|
  datetime_format = datetime.strftime('%Y-%m-%d %H:%M:%S.%L')
  "#{severity} [#{datetime_format}] ##{Process.pid}]: #{msg}\n"
end

EM.run do
  last_id = nil
  client = Mongo::Client.new(['127.0.0.1:27017'], database: 'test')
  collection = client[:github]

  # This proc will execute before script stop
  stop = proc do
    log.info 'Terminating crawler'
    EM.stop
  end

  Signal.trap('INT',  &stop)
  Signal.trap('TERM', &stop)

  # The main process
  process = proc do
    req = HttpRequest.new("https://api.github.com/users?since=#{last_id}",
                          inactivity_timeout: 5,
                          connect_timeout: 5).get(
                            head: {
                              'Authorization' => "token #{ENV['GITHUB_TOKEN']}"
                            }
                          )

    req.callback do
      req.close unless req.response_header.status == 200

      begin
        data = Yajl::Parser.parse(req.response)
        docs = data.map do |e|
          {
            github_id: e['id'],
            username: e['login'],
            type: e['type']
          }
        end

        result  = collection.insert_many(docs)
        last_id = docs.last['github_id']

        log.succsess result
      rescue Exception => e
        log.error "Processing exception: #{e}, #{e.backtrace.first(5)} \
                   Response: #{req.response_header}, #{req.response}"
      ensure
        EM.next_tick(&process)
      end
    end

    req.errback do
      log.error "Error: #{req.response_header.status}, \
                 header: #{req.response_header}, \
                 response: #{req.response}"

      EM.add_timer(3700, &process)
    end
  end

  # This proc will get last_id from main service
  init_process = proc do
    req = HttpRequest.new(ENV['LAST_ID_ENDPOINT'])
                     .get(
                       head: {
                         'Authorization' => "token #{ENV['MASTER_TOKEN']}"
                       })

    req.callback do
      req.close unless req.response_header.status == 200

      last_id = Yajl::Parser.parse(req.response)[:id]
      process.call
    end

    req.errback do
      log.error "Error: #{req.response_header.status}, \
                 header: #{req.response_header}, \
                 response: #{req.response}"

      EM.add_timer(1, &init_process)
    end
  end

  init_process.call
end
