require 'test_helper'

class CassandraObject::FinderMethodsTest < CassandraObject::TestCase
  test 'find' do
    Issue.create.tap do |issue|
      assert_equal issue, Issue.find(issue.id)
    end

    begin
      Issue.find(nil)
      assert false
    rescue => e
      assert_equal "Couldn't find Issue with key nil", e.message
    end

    assert_raise CassandraObject::RecordNotFound do
      Issue.find('what')
    end
  end

  test 'find with ids' do
    first_issue = Issue.create
    second_issue = Issue.create

    assert_equal [], Issue.find([])
    assert_equal [first_issue, second_issue].to_set, Issue.find([first_issue.id, second_issue.id]).to_set
  end

  test 'find with ids sorted' do
    ids = (0..999).to_a.map(&:to_s)
    ids.each do |i|
      IssueDynamic.create(key: i, title: "foo_title_#{i}")
    end
    ids_to_find = ids.sample(10)
    assert_equal ids_to_find, IssueDynamic.find(ids_to_find).keys
  end

  test 'find_by_id' do
    Issue.create.tap do |issue|
      assert_equal issue, Issue.find_by_id(issue.id)
    end

    assert_nil Issue.find_by_id('what')
  end

  test 'find all in batches dynamic' do
    first_issue = IssueDynamic.create(key: '1', title: 'tit', dynamic_field1: 'one', dynamic_field2: 'two')
    second_issue = IssueDynamic.create(key: '2', title: 'tit', dynamic_field1: 'one', dynamic_field2: 'two')
    res = IssueDynamic.find_all_in_batches
    reobjected = res[:results].map { |key, val| {key: key }.merge(val) }
    IssueDynamic.delete(['1', '2'])

    assert_equal [first_issue, second_issue].size, reobjected.size
  end

  test 'find by key in batches dynamic paged' do
    100.times.each do |i|
      IssueDynamic.create(key: '1', title: 'tit', "dynamic_field_#{i}" => ['a'])
      IssueDynamic.create(key: '2', title: 'tit', "dynamic_field_#{i}" => ['a'])
    end

    resp = IssueDynamic.limit(10).find_in_batches('1')
    columns = resp[:results]['1']
    assert_equal 10, columns.size
    assert_equal 'dynamic_field_0', columns.keys.first

    cursor = resp[:next_cursor]
    resp = IssueDynamic.limit(10).find_in_batches('1', cursor)
    columns = resp[:results]['1']
    assert_equal 10, columns.size
    assert_equal 'dynamic_field_18', columns.keys.first

    IssueDynamic.delete(['1', '2'])
  end

  test 'find all in batches dynamic paged' do
    issues = []
    100.times.each do |i|
      issues << IssueDynamic.create(key: i, title: 'tit', dynamic_field1: 'one', dynamic_field2: 'two')
    end

    res = []
    next_cursor = nil
    iter = 0
    loop do
      iter += 1
      resp = IssueDynamic.limit(10).find_all_in_batches(next_cursor)
      res << resp[:results].map { |key, val| {key: key.to_s }.merge(val) }
      next_cursor = resp[:next_cursor]
      break if next_cursor.nil?
    end
    res.flatten!
    IssueDynamic.delete(issues.map{|x| x[:key]})

    assert_equal issues.size, res.size
  end

  test 'first' do
    first_issue = Issue.create
    second_issue = Issue.create

    assert [first_issue, second_issue].include?(Issue.first)
  end

  test 'cql response: find with ids' do
    first_issue = Issue.create
    second_issue = Issue.create

    assert_equal [], Issue.find([])
    assert_equal [first_issue.get_cql_response, second_issue.get_cql_response].to_set, Issue.cql_response.find([first_issue.id, second_issue.id]).to_set
  end

  test 'cql response: find_by_id' do
    Issue.create.tap do |issue|
      assert_equal issue.get_cql_response, Issue.cql_response.find_by_id(issue.id)
    end

    assert_nil Issue.find_by_id('what')
  end

  # test 'cql response: all' do
  #   first_issue = Issue.create
  #   second_issue = Issue.create
  #   assert_equal [first_issue.get_cql_response, second_issue.get_cql_response].to_set, Issue.cql_response.find_all_in_batches[:results].to_set
  # end

  test 'cql response: first' do
    first_issue = Issue.create
    second_issue = Issue.create
    assert [first_issue.id, second_issue.id].include?(Issue.cql_response.first.keys.first)
  end

  test 'where' do
    # todo make better tests
    # mono parameter
    res1 = Issue.cql_response.where("column1 < 'poi'").execute
    # bi parameter
    res = Issue.cql_response.where('column1 < ?', 'poi').execute
  end

  # test 'limit in first' do
  #   first_issue = IssueDynamic.create(key: '1', title: 'tit', dynamic_field1: 'one', dynamic_field2: 'two')
  #   f = IssueDynamic.first
  # end
end
