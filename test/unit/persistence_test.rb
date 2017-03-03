#!/bin/env ruby
# encoding: utf-8

require 'test_helper'

class CassandraObject::PersistenceTest < CassandraObject::TestCase
  test 'instantiate removes unknowns' do
    assert_nil Issue.instantiate('theid', 'z' => 'nooo').attributes['z']
  end

  test 'encode_attributes' do
    assert_equal(
      {},
      Issue.encode_attributes({})
    )

    assert_equal(
      {'description' => nil},
      Issue.encode_attributes({'description' => nil})
    )

    assert_equal(
      {'description' => 'lol'},
      Issue.encode_attributes({'description' => 'lol'})
    )
  end

  test 'batch' do
    first_issue = second_issue = nil

    Issue.batch do
      assert Issue.batching?

      first_issue = Issue.create
      second_issue = Issue.create
      assert_raise(CassandraObject::RecordNotFound) { Issue.find(first_issue.id) }
      assert_raise(CassandraObject::RecordNotFound) { Issue.find(second_issue.id) }
    end

    assert !Issue.batching?
    assert_nothing_raised do
      Issue.find(first_issue.id)
    end
    assert_nothing_raised do
      Issue.find(second_issue.id)
    end
  end

  test 'persistance inquiries' do
    issue = Issue.new
    assert issue.new_record?
    assert !issue.persisted?

    issue.save
    assert issue.persisted?
    assert !issue.new_record?
  end

  test 'create' do
    issue = Issue.create { |i| i.description = 'foo' }
    assert_equal 'foo', issue.description
    assert_equal 'foo', Issue.find(issue.id).description
  end

  test 'read and write UTF' do
    utf = "\ucba1\ucba2\ucba3 ƒ´∑ƒ©√åµ≈√ˆअनुच्छेद´µøµø¬≤ 汉语漢語".force_encoding(Encoding::UTF_8)

    issue = Issue.create { |i| i.description = utf }
    assert_equal utf, issue.description
    reloaded = Issue.find(issue.id).description
    assert_equal utf, reloaded
  end

  test 'save' do
    issue = Issue.new
    issue.save

    assert_equal issue, Issue.find(issue.id)
  end

  test 'save!' do
    begin
      Issue.validates(:description, presence: true)

      record = Issue.new(description: 'bad')
      record.save!

      assert_raise CassandraObject::RecordInvalid do
        record = Issue.new
        record.save!
      end
    ensure
      Issue.reset_callbacks(:validate)
    end

  end

  test 'destroy' do
    issue = Issue.create
    issue.destroy

    assert issue.destroyed?
    assert !issue.persisted?
    assert !issue.new_record?
  end

  test 'update_attribute' do
    issue = Issue.create
    issue.update_attribute(:description, 'lol')

    assert !issue.changed?
    assert_equal 'lol', issue.description
  end

  test 'update_attributes' do
    issue = Issue.create
    issue.update_attributes(description: 'lol')

    assert !issue.changed?
    assert_equal 'lol', issue.description
  end

  test 'update_attributes!' do
    begin
      Issue.validates(:description, presence: true)

      issue = Issue.new(description: 'bad')
      issue.save!

      assert_raise CassandraObject::RecordInvalid do
        issue.update_attributes! description: ''
      end
    ensure
      Issue.reset_callbacks(:validate)
    end
  end

  test 'update nil attributes' do
    issue = Issue.create(title: 'I rule', description: 'lololol')

    issue.update_attributes title: nil

    issue = Issue.find issue.id
    assert_nil issue.title
  end

  test 'becomes' do
    klass = temp_object do
    end

    assert_kind_of klass, Issue.new.becomes(klass)
  end

  test 'reload' do
    persisted_issue = Issue.create
    fresh_issue = Issue.find(persisted_issue.id)
    fresh_issue.update_attribute(:description, 'say what')

    reloaded_issue = persisted_issue.reload
    assert_equal 'say what', persisted_issue.description
    assert_equal persisted_issue, reloaded_issue
  end

  test 'allow CQL keyword in column name' do
    assert_nothing_raised do
      Issue.string :text
      issue = Issue.create :text => 'hello'
      issue.text = 'world'
      issue.save!
      issue.text = nil
      issue.save!
    end
  end

  test 'remove' do
    record = Issue.new(title: 'cool')
    record.save!

    id = record.id
    assert_equal id, Issue.find(id).id

    Issue.remove(id)

    assert_raise CassandraObject::RecordNotFound do
      Issue.find(id)
    end
  end

  test 'remove multiple' do
    ids = []
    (1..10).each do
      record = Issue.create!(title: 'cool')
      ids << record.id
    end

    Issue.remove(ids)

    assert_equal [], Issue.find(ids)
  end

  test 'ttl' do
    record = Issue.create({title: 'name', ttl: 1})
    assert_nothing_raised do
      Issue.find(record.id)
    end

    sleep 2

    assert_raise CassandraObject::RecordNotFound do
      Issue.find(record.id)
    end
  end

  test 'dynamic create' do

    id1 = "1"
    IssueDynamic.create(key: id1, title: 'tit', dynamic_field1: 'one', dynamic_field2: 'two')
    id2 = "2"
    IssueDynamic.create(key: id2, title: 'tit2', dynamic_field1: '1', dynamic_field2: '2')
    # number of dynamic fields

    assert_equal 3, IssueDynamic.find(id1)[id1].size
  end

  test 'dynamic update' do

    id = "123"
    IssueDynamic.create(key: id, title: 'tit', dynamic_field1: 'one', dynamic_field2: 'two')
    assert_equal 3, IssueDynamic.find(id)[id].size

    IssueDynamic.update(id, {title: 'tit_new', dynamic_field1: 'new_one', dynamic_field2: nil})
    assert_equal 2, IssueDynamic.find(id)[id].size

  end

  test 'dynamic delete' do
    id = "123"
    IssueDynamic.create(key: id, title: 'tit', dynamic_field1: 'one', dynamic_field2: 'two')
    IssueDynamic.delete(id)
    assert_raise CassandraObject::RecordNotFound do
      IssueDynamic.find(id)
    end
  end

  test 'dynamic delete attributes' do
    id = "123"
    IssueDynamic.create(key: id, title: 'tit', dynamic_field1: 'one', dynamic_field2: 'two')
    IssueDynamic.delete(id, [:dynamic_field1])
    assert_equal 2, IssueDynamic.find(id)[id].size
  end

  test 'get_certain_page' do

    10.times.each do |i|
      rand = rand()
      IssueDynamic.create(key: 123098, rand => rand)
    end

    page = IssueDynamic.page(2).per_page(3).all
    assert_equal 3, page.values.first.values.size
  end

  test 'paged_request_dynamic' do

    NUMTEST = 21000
    KEY = '987987'

    NUMTEST.times.each do |i|
      rand = rand()
      IssueDynamic.create(key: KEY, rand => rand)
    end
    found = IssueDynamic.find_by_id(KEY)

    assert_equal NUMTEST, found[KEY].size
  end

end
