require 'test_helper'

class CassandraObject::TimestampsTest < CassandraObject::TestCase
  test 'timestamps set on create' do
    issue = Issue.create

    assert_in_delta Time.now.to_i, issue.created_at.to_i, 10
    assert_in_delta Time.now.to_i, issue.updated_at.to_i, 10
  end

  test 'updated_at set on change' do
    issue = Issue.create

    issue.updated_at = nil
    issue.description = 'lol'
    issue.save

    assert_in_delta Time.now.to_i, issue.updated_at.to_i, 10
  end

  test 'created_at sets only if nil' do
    time = 5.days.ago
    issue = Issue.create created_at: time
    assert_equal time, issue.created_at
  end

  test 'set updated_at to now when not passed as an attribute' do
    udate = 1.year.ago
    issue = Issue.create(description: 'foo', updated_at: udate)
    assert_equal udate, issue.updated_at
    issue.update_attributes(description: 'test')
    assert_not_equal udate, issue.updated_at
  end

  test 'set updated_at to passed value' do
    issue = Issue.create(description: 'foo')
    updated_at = issue.updated_at
    new_updated_at = updated_at + 5.days
    issue.update_attributes(description: 'bar', store_updated_at: new_updated_at)
    assert_equal new_updated_at, issue.updated_at
  end

  test 'set updated_at to passed value even if is equal to the stored value' do
    udate = 1.year.ago
    issue = Issue.create(description: 'foo', updated_at: udate)
    assert_equal udate, issue.updated_at
    issue.update_attributes(description: 'bar', store_updated_at: issue.updated_at)
    assert_equal udate, issue.updated_at
  end
end
