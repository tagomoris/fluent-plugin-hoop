require 'helper'

class HoopOutputReconnectTest < Test::Unit::TestCase
  # setup/teardown and tests of dummy hoop server defined at the end of this class...

  CONFIG = %[
    hoop_server localhost:14000
    path /logs/from/fluentd/foo-%Y%m%d
    username hoopuser
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::TimeSlicedOutputTestDriver.new(Fluent::HoopOutput).configure(conf)
  end

  def test_write
    d = create_driver CONFIG

    assert_equal '404', get_code('localhost', 14000, '/logs/from/fluentd/foo-20111124', {'Cookie' => VALID_COOKIE_STRING})

    time = Time.parse("2011-10-01 00:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)
    paths = d.run
    assert_equal ['/logs/from/fluentd/foo-20111001'], paths
    assert_equal %[2011-10-01T00:14:15Z\ttest\t{"a":1}\n2011-10-01T00:14:15Z\ttest\t{"a":2}\n], get_content('localhost', 14000, paths.first, {'Cookie' => VALID_COOKIE_STRING})

#    d = create_driver CONFIG
    restart_dummy_server

    time = Time.parse("2011-10-02 00:14:15 UTC").to_i
    d.emit({"a"=>3}, time)
    d.emit({"a"=>4}, time)
    paths = d.run

    assert_equal ['/logs/from/fluentd/foo-20111001', '/logs/from/fluentd/foo-20111002'], paths.sort
    assert_equal %[2011-10-02T00:14:15Z\ttest\t{"a":3}\n2011-10-02T00:14:15Z\ttest\t{"a":4}\n], get_content('localhost', 14000, paths.sort.last, {'Cookie' => VALID_COOKIE_STRING})

  end

  VALID_COOKIE_STRING = 'alfredo.auth="u=hoopuser&p=hoopuser&t=simple&e=1322203001386&s=SErpv88rOAVEItSOIoCtIV/DSpE="'
  RES_COOKIE_AUTH_FAILURE = WEBrick::Cookie.parse_set_cookie('alfredo.auth=""; Expires=Thu, 01-Jan-1970 00:00:10 GMT; Path=/')
  RES_COOKIE_AUTH_SUCCESS = WEBrick::Cookie.parse_set_cookie(VALID_COOKIE_STRING + '; Version=1; Path=/')
  RES_BODY_STATUS_ROOT = '{"path":"http:\/\/localhost:14000\/","isDir":true,"len":0,"owner":"hoopuser","group":"supergroup","permission":"-rwxr-xr-x","accessTime":0,"modificationTime":1320055230010,"blockSize":0,"replication":0}'
  RES_FORMAT_ALREADY_EXISTS = "{\"statusCode\":500,\"reason\":\"Internal Server Error\",\"message\":\"java.io.IOException: failed to create file %s on client 127.0.0.1 either because the filename is invalid or the file exists\",\"exception\":\"org.apache.hadoop.ipc.RemoteException\"}"
  RES_FORMAT_NOT_FOUND = "{\"statusCode\":404,\"reason\":\"Not Found\",\"message\":\"java.io.FileNotFoundException: failed to append to non-existent file %s on client 127.0.0.1\",\"exception\":\"java.io.FileNotFoundException\"}"
  RES_FORMAT_NOT_FOUND_GET = "{\"statusCode\":404,\"reason\":\"Not Found\",\"message\":\"File does not exist: %s\",\"exception\":\"java.io.FileNotFoundException\"}"

  CONTENT_TYPE_JSON = 'application/json'

  def start_server
    @dummy_server_thread = Thread.new do
      srv = if ENV['FLUENT_TEST_DEBUG']
              logger = WEBrick::Log.new('/dev/null', WEBrick::BasicLog::DEBUG)
              WEBrick::HTTPServer.new({:BindAddress => '127.0.0.1', :Port => 14000, :Logger => logger, :AccessLog => []})
            else
              WEBrick::HTTPServer.new({:BindAddress => '127.0.0.1', :Port => 14000})
            end
      @fsdata = {}
      begin
        srv.mount_proc('/'){|req,res|
          # status only...
          if req.query['user.name'] or req.cookies.index{|item| item.name == 'alfredo.auth' and item.value}
            res.status = 200
            res.content_type = CONTENT_TYPE_JSON
            res.cookies << RES_COOKIE_AUTH_SUCCESS
            res.body = RES_BODY_STATUS_ROOT
          else
            res.cookies << RES_COOKIE_AUTH_FAILURE
            res.status = 401
          end
        }
        srv.mount_proc('/logs/from/fluentd') {|req, res|
          if req.request_method == 'POST' or req.request_method == 'PUT' or req.request_method == 'DELETE'
            # WEBrick's default handler ignores query parameter of URI without method GET
            req.query.update(Hash[*(req.request_line.split(' ')[1].split('?')[1].split('&').map{|kv|kv.split('=')}.flatten)])
          end
          case
          when (not req.query['user.name'] and req.cookies.index{|item| item.name == 'alfredo.auth' and item.value} < 0)
            res.cookies << RES_COOKIE_AUTH_FAILURE
            res.status = 401
          when (req.query['op'] == 'create' and @fsdata[req.path] and req.query['overwrite'] and req.query['overwrite'] == 'false')
            res.status = 500
            res.content_type = CONTENT_TYPE_JSON
            res.body = sprintf RES_FORMAT_ALREADY_EXISTS, req.path
          when req.query['op'] == 'create'
            @fsdata[req.path] = req.body
            res.status = 201
            res['Location'] = 'http://localhost:14000' + req.path
            res.content_type = CONTENT_TYPE_JSON
          when (req.query['op'] == 'append' and @fsdata[req.path])
            @fsdata[req.path] += req.body
            res.status = 200
            res['Location'] = 'http://localhost:14000' + req.path
            res.content_type = CONTENT_TYPE_JSON
          when req.query['op'] == 'append'
            res.status = 404
            res.content_type = CONTENT_TYPE_JSON
            res.body = sprintf RES_FORMAT_NOT_FOUND, req.path
          when (req.request_method == 'GET' and @fsdata[req.path]) # maybe GET
            res.status = 200
            res.content_type = 'application/octet-stream'
            res.body = @fsdata[req.path]
          else
            res.status = 404
            res.content_type = CONTENT_TYPE_JSON
            res.body = sprintf RES_FORMAT_NOT_FOUND_GET, req.path
          end
        }
        srv.start
      ensure
        srv.shutdown
      end
    end

    # to wait completion of dummy server.start()
    require 'thread'
    cv = ConditionVariable.new
    watcher = Thread.new {
      connected = false
      while not connected
        begin
          get_content('localhost', 14000, '/', {'Cookie' => VALID_COOKIE_STRING})
          connected = true
        rescue Errno::ECONNREFUSED
          sleep 0.1
        rescue StandardError => e
          p e
          sleep 0.1
        end
      end
      cv.signal
    }
    mutex = Mutex.new
    mutex.synchronize {
      cv.wait(mutex)
    }
  end

  def setup
    Fluent::Test.setup
    start_server
  end

  def restart_dummy_server
    @dummy_server_thread.kill
    @dummy_server_thread.join
    start_server
  end

  def test_dummy_server
    d = create_driver
    authheader = {'Cookie' => VALID_COOKIE_STRING}
    client = Net::HTTP.start(d.instance.hoop_server.split(':')[0], d.instance.hoop_server.split(':')[1])
    assert_equal '401', client.request_get('/').code
    assert_equal '200', client.request_get('/?user.name=hoopuser').code
    assert_equal '200', client.request_get('/', authheader).code

    # /logs/from/fluentd
    path1 = '/logs/from/fluentd/hoge001/moge-access-log'
    path1_line1 = "1111111111111111111111111111111\n"
    path1_line2 = "2222222222222222222222222222222222222222222222222\n"
    assert_equal '404', client.request_put(path1 + '?op=append', path1_line1, authheader).code
    assert_equal '201', client.request_post(path1 + '?op=create&overwrite=false', path1_line1, authheader).code
    assert_equal path1_line1, client.request_get(path1, authheader).body
    assert_equal '200', client.request_put(path1 + '?op=append', path1_line2, authheader).code
    assert_equal path1_line1 + path1_line2, client.request_get(path1, authheader).body

    path2 = '/logs/from/fluentd/hoge002/moge-access-log'
    path2_line1 = "XXXXX___1111111111111111111111111111111\n"
    path2_line2 = "YYYYY___2222222222222222222222222222222222222222222222222\n"
    assert_equal '404', client.request_put(path2 + '?op=append', path2_line1, authheader).code
    assert_equal '201', client.request_post(path2 + '?op=create&overwrite=false', path2_line1, authheader).code
    assert_equal '500', client.request_post(path2 + '?op=create&overwrite=false', path2_line1, authheader).code
    assert_equal path2_line1, client.request_get(path2, authheader).body
    assert_equal '200', client.request_put(path2 + '?op=append', path2_line2, authheader).code
    assert_equal path2_line1 + path2_line2, client.request_get(path2, authheader).body
    assert_equal path2_line1 + path2_line2, get_content('localhost', 14000, path2, authheader)
  end

  def teardown
    @dummy_server_thread.kill
    @dummy_server_thread.join
  end
end
