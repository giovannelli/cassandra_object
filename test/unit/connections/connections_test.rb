#!/bin/env ruby
# encoding: utf-8

require 'test_helper'

class CassandraObject::ConnectionsTest < CassandraObject::TestCase

  test 'test connections' do
    IssueSchema.delete_all
    ids = []
    (1..10000).each do
      i = IssueSchema.create(title: "fadjfjkadhsfkjldsa")
      ids << i.id
    end

    threads = []

    (0..10).collect do |i|
      thr = Thread.new do
        begin
          IssueSchema.find(ids)
        rescue Exception => e
          puts("\n\n\n\n" + e.message)

        end
      end
      threads << thr
    end

  end

  # test 'test create' do
  #
  #   values = []
  #   threads = []
  #   (0..100).collect do |i|
  #
  #
  #     puts "spawn thread #{i}"
  #     thr = Thread.new do
  #       begin
  #         values << Issue.new(title: 'title', description: 'desc').search.results.size
  #       rescue Exception => e
  #         puts("\n\n\n\n" + e.message)
  #         retry
  #       end
  #     end
  #     threads << thr
  #   end
  #
  # end

end
