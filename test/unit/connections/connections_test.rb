#!/bin/env ruby
# encoding: utf-8

require 'test_helper'

class CassandraObject::ConnectionsTest < CassandraObject::TestCase

  test 'test connections' do
    issues = []
    tot_op = 0
    operations = 0

    tasklist = []
    # Set the threads going
    10.times do |n|
      task = Thread.new(n) { |x|
        1000.times do
          issue = Issue.create(title: "#{n} - #{x}")
          issues << issue.id
          # sleep 1
          tot_op += 1
          operations += 1
        end
      }
      tasklist << task
    end

    n_alive = 1
    while(n_alive > 0) do
      n_alive = tasklist.select{|t| t.alive?}.size
      puts "ops/sec: #{operations} tot ops: #{tot_op}, running threads: #{n_alive}"
      operations = 0
      sleep 1
    end

    # Wait for the threads to finish
    tasklist.each { |task| task.join }

    # readall
    puts "issues tot: #{issues.size}"

    byebug
  end

end
