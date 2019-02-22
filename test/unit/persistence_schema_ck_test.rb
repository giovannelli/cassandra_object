#!/bin/env ruby
# encoding: utf-8

require 'test_helper'

class CassandraObject::PersistenceSchemaCkTest < CassandraObject::TestCase
  test 'composite key' do
    time1 = Time.now
    time2 = time1 + 1.second

    IssueSchemaCk.create(id: '1', type: 'first', date: time1, value: 1.to_f)
    IssueSchemaCk.create(id: '1', type: 'second', date: time1, value: 1.to_f)
    IssueSchemaCk.create(id: '1', type: 'first', date: time2, value: 2.to_f)
    IssueSchemaCk.create(id: '1', type: 'second', date: time2, value: 2.to_f)

    res = IssueSchemaCk.where('type = ?', 'first').find_by_id([1])
    assert_equal 2, res.size
    assert_equal 1, res.first.value

    item = res[0]
    assert_equal '1', item.id
    assert_equal time1.to_i, item.date.to_i

    item = res[1]
    assert_equal '1', item.id
    assert_equal time2.to_i, item.date.to_i
  end

  test 'delete' do
    IssueSchemaCk.create(id: '1', type: 'first', date: Time.now, value: 1.to_f)
    IssueSchemaCk.create(id: '1', type: 'second', date: Time.now, value: 1.to_f)
    IssueSchemaCk.delete('1')
    assert_equal 0, IssueSchemaCk.find_by_id([1]).size
  end

  test 'delete with attributes' do
    time = Time.now - 10.days
    IssueSchemaCk.create(id: '1', type: 'first', date: time, value: 1.to_f)
    IssueSchemaCk.create(id: '1', type: 'first', date: Time.now, value: 1.to_f)
    IssueSchemaCk.create(id: '2', type: 'first', date: time, value: 1.to_f)
    IssueSchemaCk.create(id: '2', type: 'first', date: Time.now, value: 1.to_f)

    IssueSchemaCk.delete('1', type: 'first')
    assert_equal 2, IssueSchemaCk.find_by_id([1,2]).size
  end

  test 'delete multiple' do
    IssueSchemaCk.create(id: '1', type: 'first', date: Time.now, value: 1.to_f)
    IssueSchemaCk.create(id: '1', type: 'second', date: Time.now, value: 1.to_f)
    IssueSchemaCk.create(id: '2', type: 'first', date: Time.now, value: 1.to_f)
    IssueSchemaCk.create(id: '2', type: 'first', date: Time.now, value: 1.to_f)

    IssueSchemaCk.delete(['1','2'])
    assert_equal 0, IssueSchemaCk.find_by_id([1]).size
    assert_equal 0, IssueSchemaCk.find_by_id([2]).size
  end

  test 'destroy' do
    IssueSchemaCk.create(id: '1', type: 'first', date: Time.now, value: 1.to_f)
    IssueSchemaCk.create(id: '1', type: 'second', date: Time.now, value: 1.to_f)

    IssueSchemaCk.find_by_id(['1']).first.destroy
    assert_equal 1, IssueSchemaCk.find_by_id([1]).size
  end
end
