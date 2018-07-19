#!/bin/env ruby
# encoding: utf-8

require 'test_helper'

class CassandraObject::PersistenceSchemaCkTest < CassandraObject::TestCase

  test 'create' do
    IssueSchemaCk.create(id: '1', type: 'first', date: Time.now, value: 1.to_f)
    IssueSchemaCk.create(id: '1', type: 'second', date: Time.now, value: 1.to_f)
    sleep 1
    IssueSchemaCk.create(id: '1', type: 'first', date: Time.now, value: 2.to_f)
    IssueSchemaCk.create(id: '1', type: 'second', date: Time.now, value: 2.to_f)
    sleep 1
    IssueSchemaCk.create(id: '1', type: 'first', date: Time.now, value: 3.to_f)
    IssueSchemaCk.create(id: '1', type: 'second', date: Time.now, value: 3.to_f)

    res = IssueSchemaCk.where('type = ?', 'first').find_by_id([1])

    assert_equal 3, res.size
    assert_equal 1, res.first.value
  end

end
