require 'test_helper'

class CassandraObject::Scope::QueryMethodsTest < CassandraObject::TestCase
  test 'select' do
    original_issue = Issue.create title: 'foo', description: 'bar'
    found_issue =  Issue.select(:title).find(original_issue.id)

    assert_equal 'foo', found_issue.title
    assert_equal original_issue.id, found_issue.id
    assert_nil found_issue.description
  end

  test 'select with block' do
    foo_issue = Issue.create title: 'foo'
    Issue.create title: 'bar'
    assert_equal [foo_issue], Issue.all.select { |issue| issue.title == 'foo' }
  end

  test 'chaining where with scope' do
    issue = Issue.create title: 'abc', description: 'def'
    query = Issue.select(:title).for_key(issue.id)

    assert_equal [:title], query.select_values
  end

  test 'only column names' do
    Issue.create title: 'foo', description: 'bar'
    foo_issue_columns =  Issue.columns.first
    assert_equal ['created_at', 'description', 'title', 'updated_at'], foo_issue_columns[foo_issue_columns.keys.first]
  end

end
