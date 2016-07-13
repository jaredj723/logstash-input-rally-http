# encoding: utf-8
require 'logstash/inputs/base'
require 'logstash/namespace'
require 'logstash/plugin_mixins/http_client'
require 'socket' # for Socket.gethostname
require 'manticore'

# This Logstash input plugin allows you to call an HTTP API, decode the output of it into event(s), and
# send them on their merry way. The idea behind this plugins came from a need to read springboot
# metrics endpoint, instead of configuring jmx to monitor my java application memory/gc/ etc.
#
# ==== Example
# Reads from a list of urls and decodes the body of the response with a codec.
# The config should look like this:
#
# [source,ruby]
# ----------------------------------
# input {
#   rally_http {
#     urls => {
#       test1 => "http://localhost:9200"
#       test2 => {
#         # Supports all options supported by ruby's Manticore HTTP client
#         method => get
#         url => "http://localhost:9200/_cluster/health"
#         headers => {
#           Accept => "application/json"
#         }
#         auth => {
#           user => "AzureDiamond"
#           password => "hunter2"
#         }
#       }
#     }
#     request_timeout => 60
#     interval => 60
#     codec => "json"
#     # A hash of request metadata info (timing, response headers, etc.) will be sent here
#     metadata_target => "rally_http_metadata"
#   }
# }
#
# output {
#   stdout {
#     codec => rubydebug
#   }
# }
# ----------------------------------

class LogStash::Inputs::Rally_HTTP < LogStash::Inputs::Base
  include LogStash::PluginMixins::HttpClient

  config_name 'rally_http'

  default :codec, 'json'

  # A Hash of urls in this format : `"name" => "url"`.
  # The name and the url will be passed in the outputed event
  config :urls, :validate => :hash, :required => true

  # How often (in seconds) the urls will be called
  config :interval, :validate => :number, :required => true

  # Define the target field for placing the received data. If this setting is omitted, the data will be stored at the root (top level) of the event.
  config :target, :validate => :string

  # If you'd like to work with the request/response metadata.
  # Set this value to the name of the field you'd like to store a nested
  # hash of metadata.
  config :metadata_target, :validate => :string, :default => '@metadata'

  public
  def register
    @host = Socket.gethostname.force_encoding(Encoding::UTF_8)

    @logger.info('Registering Rally HTTP Input', :type => @type,
                 :urls => @urls, :interval => @interval, :timeout => @timeout)
    setup_requests!
  end

  def stop
    Stud.stop!(@interval_thread) if @interval_thread
  end

  private
  def setup_requests!
    @requests = Hash[@urls.map { |name, url| [name, normalize_request(url)] }]
  end

  private
  def normalize_request(url_or_spec)
    if url_or_spec.is_a?(String)
      res = [:get, url_or_spec]
    elsif url_or_spec.is_a?(Hash)
      # The client will expect keys / values
      spec = Hash[url_or_spec.clone.map { |k, v| [k.to_sym, v] }] # symbolize keys

      # method and url aren't really part of the options, so we pull them out
      method = (spec.delete(:method) || :get).to_sym.downcase
      url = spec.delete(:url)

      # Convert the paging options to keywords, if they exist.
      spec[:paging] = {size_param: spec[:paging]['size_param'], size_value: spec[:paging]['size_value'],
                       index_param: spec[:paging]['index_param'], index_value: spec[:paging]['index_value']} if spec[:paging]

      # Ensure that the values from the paging options are used as the params
      spec[:params][spec[:paging][:index_param]] = spec[:paging][:index_value] if spec[:paging]
      spec[:params][spec[:paging][:size_param]] = spec[:paging][:size_value] if spec[:paging]


      # We need these strings to be keywords!
      spec[:auth] = {user: spec[:auth]['user'], pass: spec[:auth]['password']} if spec[:auth]

      res = [method, url, spec]
    else
      raise LogStash::ConfigurationError, "Invalid URL or request spec: '#{url_or_spec}', expected a String or Hash!"
    end

    validate_request!(url_or_spec, res)
    res
  end

  private
  def validate_request!(url_or_spec, request)
    method, url, spec = request

    raise LogStash::ConfigurationError, "Invalid URL #{url}" unless URI::DEFAULT_PARSER.regexp[:ABS_URI].match(url)

    raise LogStash::ConfigurationError, "No URL provided for request! #{url_or_spec}" unless url
    if spec && spec[:auth]
      unless spec[:auth][:user]
        raise LogStash::ConfigurationError, "Auth was specified, but 'user' was not!"
      end
      unless spec[:auth][:pass]
        raise LogStash::ConfigurationError, "Auth was specified, but 'password' was not!"
      end
    end

    request
  end

  public
  def run(output_queue)
    # TODO: Support cron-based intervals
    # @interval_thread = Thread.current
    # Stud.interval(@interval) do
    run_once(output_queue)
    # end
  end

  private
  def run_once(output_queue)
    @requests.each do |name, request|
      request_sync(output_queue, name, request)
    end
    # Execute all of the queued async requests.
    client.execute!
  end

  private
  def request_async(output_queue, name, request)
    @logger.debug? && @logger.debug('Fetching URL', :name => name, :url => request)
    started = Time.now

    method, *request_opts = request
    client.async.send(method, *request_opts).
        on_success { |response| handle_success(output_queue, name, request, response, Time.now - started) }.
        on_failure { |exception| handle_failure(output_queue, name, request, exception, Time.now - started) }
  end

  private
  def request_sync(output_queue, name, request)
    @logger.debug? && @logger.debug('Fetching sync URL', :name => name, :url => request)
    started = Time.now

    method, *request_opts = request
    client.send(method, *request_opts).
        on_success { |response| handle_success(output_queue, name, request, response, Time.now - started) }.
        on_failure { |exception| handle_failure(output_queue, name, request, exception, Time.now - started) }.call
  end

  private
  def handle_success(output_queue, name, request, response, execution_time)
    @codec.decode(response.body) do |decoded|
      event = @target ? LogStash::Event.new(@target => decoded.to_hash) : decoded
      handle_decoded_event(output_queue, name, request, response, event, execution_time)
    end
  end

  private
  def handle_decoded_event(output_queue, name, request, response, event, execution_time)
    method, url, paged_spec = request.clone
    if !paged_spec.key?(:paging)
      parse_events(output_queue, name, request, response, event, execution_time)
    else
      handle_paging(output_queue, name, request, event)
    end

  rescue StandardError, java.lang.Exception => e
    @logger.error? && @logger.error('Error eventifying response!',
                                    :exception => e,
                                    :exception_message => e.message,
                                    :name => name,
                                    :url => request,
                                    :response => response
    )
  end

  private
  def handle_paging(output_queue, name, request, event)
    method, url, paged_spec = request.clone
    start_index = paged_spec[:paging][:index_value]
    page_size = paged_spec[:paging][:size_value]
    index_param = paged_spec[:paging][:index_param]

    # Get the total count so that we can generate all of the events up front.
    if !event['QueryResult'].nil?
      total_results = event['QueryResult']['TotalResultCount']
    else
      total_results = event['TotalResultCount']
    end

    @logger.debug? && @logger.debug('Generating additional requests for start index and total results', :start_index => start_index, :total_results => total_results)
    (start_index..total_results).step(page_size) do |new_index|
      @logger.debug? && @logger.debug('Generating request for index', :new_index => new_index)
      request_paged_spec = paged_spec.clone
      request_paged_spec.delete(:paging)
      # Bump the index by the pagesize
      request_paged_spec[:params][index_param] = new_index
      request_async(output_queue, name, [method, url, request_paged_spec])
      client.execute!
    end
  end

  private
  def parse_events(output_queue, name, request, response, event, execution_time)
    # Determine where to parse the events from based on the contents of the event.
    if !event['QueryResult'].nil? && !event['QueryResult']['Results'].nil? && event['QueryResult']['Results'].length > 0
      queue_events(output_queue, name, request, response, event['QueryResult']['Results'], execution_time)
    elsif !event['Results'].nil? && event['Results'].length > 0
      queue_events(output_queue, name, request, response, event['Results'], execution_time)
    end
  end

  private
  def queue_events(output_queue, name, request, response, results, execution_time)
    results.each do |result|
      @logger.debug? && @logger.debug('Trying to turn query result into event', :result => result)
      result_event = LogStash::Event.new(result)
      create_event(output_queue, name, request, response, result_event, execution_time)
    end
  end

=begin
  private
  def handle_paging(output_queue, name, request, response, event, execution_time)
    method, url, paged_spec = request.clone

    # No need to continue if the paging parameters are not defined.
    return unless paged_spec.key?(:paging)

    # Capture some of the elements that will be reused multiple times in this function.
    page_size = paged_spec[:paging][:size_value]
    index_param = paged_spec[:paging][:index_param]

    # TODO: allow for offset-only paging (like testrail)
    # TODO: This is Rally specific at the moment.
    # TODO: Handle pagination better: https://rally1.rallydev.com/analytics/doc/#/manual/34affd7a77e457af87578e4112955f08
    # Determine if we need to perform paging
    continue = false
    if !event['HasMore'].nil? && (event['HasMore']) # TODO: Might be able to extract the field that represents "more" values.
      continue = true
    else # Otherwise, might be able to extract which field can be used to count up the elements.
      if !event['QueryResult'].nil? && !event['QueryResult']['Results'].nil? && event['QueryResult']['Results'].length > 0
        continue = (page_size == event['QueryResult']['Results'].length)
      elsif !event['Results'].nil? && event['Results'].length > 0
        continue = (page_size == event['Results'].length)
      end
    end

    # If we received a full page of results, send another request.
    if continue
      @logger.debug? && @logger.debug('More results detected for request', :request => request)

      # Bump the index by the pagesize
      paged_spec[:params][index_param] = paged_spec[:params][index_param] + page_size
      paged_request = [method, url, paged_spec]
      request_sync(output_queue, name, paged_request)
    end
  end
=end

  private
  def create_event(output_queue, name, request, response, event, execution_time)
    # TODO: Strip leading _ from field names.
    event['rallyType'] = event.remove('_type') unless event['_type'].nil?
    apply_metadata(event, name, request, response, execution_time)
    decorate(event)
    output_queue << event
  end

  private
  # Beware, on old versions of manticore some uncommon failures are not handled
  def handle_failure(output_queue, name, request, exception, execution_time)
    event = LogStash::Event.new
    apply_metadata(event, name, request)

    event.tag('_http_request_failure')

    # This is also in the metadata, but we send it anyone because we want this
    # persisted by default, whereas metadata isn't. People don't like mysterious errors
    event['http_request_failure'] = {
        'request' => structure_request(request),
        'name' => name,
        'error' => exception.to_s,
        'backtrace' => exception.backtrace,
        'runtime_seconds' => execution_time
    }

    output_queue << event
  rescue StandardError, java.lang.Exception => e
    @logger.error? && @logger.error('Cannot read URL or send the error as an event!',
                                    :exception => e,
                                    :exception_message => e.message,
                                    :exception_backtrace => e.backtrace,
                                    :name => name,
                                    :url => request
    )
  end

  private
  def apply_metadata(event, name, request, response=nil, execution_time=nil)
    return unless @metadata_target
    event[@metadata_target] = event_metadata(name, request, response, execution_time)
  end

  private
  def event_metadata(name, request, response=nil, execution_time=nil)
    m = {
        'name' => name,
        'host' => @host,
        'request' => structure_request(request),
    }

    m['runtime_seconds'] = execution_time

    if response
      m['code'] = response.code
      m['response_headers'] = response.headers
      m['response_message'] = response.message
      m['times_retried'] = response.times_retried
    end

    m
  end

  private
  # Turn [method, url, spec] requests into a hash for friendlier logging / ES indexing
  def structure_request(request)
    method, url, spec = request
    # Flatten everything into the 'spec' hash, also stringify any keys to normalize
    Hash[(spec||{}).merge({
                              'method' => method.to_s,
                              'url' => url,
                          }).map { |k, v| [k.to_s, v] }]
  end
end
