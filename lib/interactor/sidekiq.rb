# frozen_string_literal: true

require 'interactor'
require 'sidekiq'
require 'active_support/core_ext/hash'

module Interactor
  # Internal: Install Interactor's behavior in the given class.
  def self.included(base)
    base.class_eval do
      extend ClassMethods
      extend SidekiqWorker
      include Hooks
      include SidekiqWorkerConfiguration

      # Public: Gets the Interactor::Context of the Interactor instance.
      attr_reader :context
    end
  end

  # based on Sidekiq 4.x #delay method, which is not enabled by default in Sidekiq 5.x
  # https://github.com/mperham/sidekiq/blob/4.x/lib/sidekiq/extensions/generic_proxy.rb
  # https://github.com/mperham/sidekiq/blob/4.x/lib/sidekiq/extensions/class_methods.rb

  module SidekiqWorker
    class Worker
      include ::Sidekiq::Worker

      sidekiq_retries_exhausted do |job, e|
        return if job['args'].blank? || job['args'].first['interactor_class'].blank?

        interactor_class = job['args'].first['interactor_class'].constantize
        if interactor_class.respond_to?(:handle_sidekiq_retries_exhausted)
          interactor_class.handle_sidekiq_retries_exhausted(job, e)
        else
          raise e
        end
      end

      def perform(interactor_context)
        interactor_class(interactor_context).sync_call(interactor_context.reject { |c| ['interactor_class'].include? c.to_s })
      rescue Exception => e
        if interactor_class(interactor_context).respond_to?(:handle_sidekiq_exception)
          interactor_class(interactor_context).handle_sidekiq_exception(e)
        else
          raise e
        end
      end

      private

      def interactor_class(interactor_context)
        Module.const_get interactor_context[:interactor_class]
      end
    end

    def sync_call(interactor_context = {})
      new(interactor_context).tap(&:run!).context
    end

    def async_call(interactor_context = {})
      options = handle_sidekiq_options(interactor_context)
      schedule_options = delay_sidekiq_schedule_options(interactor_context)

      worker_class.set(options).perform_in(schedule_options.fetch(:delay, 0), handle_context_for_sidekiq(interactor_context))
      new(interactor_context.to_h).context
    rescue Exception => e
      begin
        new(interactor_context.to_h).context.fail!(error: e.message)
      rescue Failure => e
        e.context
      end
    end

    private

    def worker_class
      return Worker if !defined?(@custom_sidekiq_worker_class) || @custom_sidekiq_worker_class.nil?

      return @custom_sidekiq_worker_class if @custom_sidekiq_worker_class < Worker

      raise "#{klass} is not a valid Sidekiq worker class. It must be a subclass of ::Interactor::SidekiqWorker::Worker."
    end

    def handle_context_for_sidekiq(interactor_context)
      interactor_context.to_h.merge(interactor_class: to_s).except(:sidekiq_options).except(:sidekiq_schedule_options).stringify_keys
    end

    def handle_sidekiq_options(interactor_context)
      if interactor_context[:sidekiq_options].nil?
        respond_to?(:sidekiq_options) ? sidekiq_options : { queue: :default }
      else
        interactor_context[:sidekiq_options]
      end
    end

    def delay_sidekiq_schedule_options(interactor_context)
      options = handle_sidekiq_schedule_options(interactor_context)
      return {} unless options.key?(:perform_in) || options.key?(:perform_at)

      { delay: options[:perform_in] || options[:perform_at] }
    end

    def handle_sidekiq_schedule_options(interactor_context)
      if interactor_context[:sidekiq_schedule_options].nil?
        respond_to?(:sidekiq_schedule_options) ? sidekiq_schedule_options : { delay: 0 }
      else
        interactor_context[:sidekiq_schedule_options]
      end
    end
  end

  module SidekiqWorkerConfiguration
    def self.included(base)
      base.class_eval do
        extend ClassMethods
      end
    end

    module ClassMethods
      def sidekiq_worker_class(klass)
        @custom_sidekiq_worker_class = klass
      end
    end
  end

  module Async
    def self.included(base)
      base.class_eval do
        include Interactor

        extend ClassMethods
      end
    end

    module ClassMethods
      def call(interactor_context = {})
        default_async_call(interactor_context)
      end

      def call!(interactor_context = {})
        default_async_call(interactor_context)
      end

      private

      def default_async_call(interactor_context)
        async_call(interactor_context)
      end
    end
  end
end
