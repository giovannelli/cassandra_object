#!/bin/env ruby
# encoding: utf-8

require 'test_helper'

class CassandraObject::PersistenceSchemaTest < CassandraObject::TestCase

  test 'instantiate removes unknowns' do
    assert_nil IssueSchema.instantiate('theid', 'z' => 'nooo').attributes['z']
  end

  test 'create' do
    issue = IssueSchema.create title: 'tit', description: 'foo'
    assert_equal 'foo', issue.description
    assert_equal 'foo', IssueSchema.find(issue.id).description
  end

  test 'save' do
    issue = IssueSchema.new
    issue.title = 'tit'
    issue.save

    assert_equal issue, IssueSchema.find(issue.id)
  end

  test 'save!' do
    record = IssueSchema.new(title: 'tit', description: 'bad')
    record.save!

    assert_raise CassandraObject::RecordInvalid do
      record = IssueSchema.new(description: 'lololol')
      record.save!
    end

  end

  test 'destroy' do
    issue = IssueSchema.create(title: 'I rule', description: 'lololol')
    issue.destroy

    assert issue.destroyed?
    assert !issue.persisted?
    assert !issue.new_record?
  end

  test 'update_attribute' do
    issue = IssueSchema.create(title: 'I rule', description: 'lololol')
    issue.update_attribute(:description, 'lol')

    assert !issue.changed?
    assert_equal 'lol', issue.description
  end

  test 'update_attributes' do
    issue = IssueSchema.create(title: 'I rule', description: 'lololol')
    issue.update_attributes(title: 'lollete', description: 'lol')

    assert !issue.changed?
    assert_equal 'lol', issue.description
  end

  test 'update_attributes!' do
    issue = IssueSchema.new(description: 'bad')
    issue.title = 'titttt'
    issue.save!

    assert_raise CassandraObject::RecordInvalid do
      issue.update_attributes! title: ''
    end
  end

  test 'update nil attributes' do
    issue = IssueSchema.create(title: 'I rule', description: 'lololol')

    issue.update_attributes description: nil

    issue = IssueSchema.find issue.id
    assert_nil issue.description
  end

  test 'becomes' do
    klass = temp_object do
    end

    assert_kind_of klass, IssueSchema.new.becomes(klass)
  end

  test 'reload' do
    persisted_issue = IssueSchema.create(title: 'I rule', description: 'lololol')
    fresh_issue = IssueSchema.find(persisted_issue.id)
    fresh_issue.update_attribute(:description, 'say what')

    reloaded_issue = persisted_issue.reload
    assert_equal 'say what', persisted_issue.description
    assert_equal persisted_issue, reloaded_issue
  end

  # test 'remove' do
  #   issue = IssueSchema.create(title: 'I rule', description: 'lololol')
  #   id = issue.id
  #   assert_equal id, IssueSchema.find(id).id
  #   IssueSchema.remove(id)
  #   assert_raise CassandraObject::RecordNotFound do
  #     IssueSchema.find(id)
  #   end
  # end

  # test 'remove multiple' do
  #   ids = []
  #   (1..10).each do
  #     issue = IssueSchema.create(title: 'I rule', description: 'lololol')
  #     ids << issue.id
  #   end

  #   IssueSchema.remove(ids)

  #   assert_equal [], IssueSchema.find(ids)
  # end

  test 'ttl' do
    description_test = 'this is the one with ttl'
    issue = IssueSchema.create(title: 'I rule', description: description_test, ttl: 1)
    assert_nothing_raised do
      IssueSchema.find(issue.id)
    end
    sleep 2
    issue = IssueSchema.find(issue.id) rescue nil
    unless issue.nil?
      assert_not_equal issue.description, description_test
    end
  end

  test 'type tests' do
    issue = IssueSchema.create(title: 'title', description: 'desc', field: 1.5, intero: 10)
    assert_nothing_raised do
      IssueSchema.find(issue.id)
    end

    from_db = IssueSchema.find(issue.id)
    assert_equal Float, from_db.field.class
    assert_equal Integer, from_db.intero.class
    # TODO add other types
    # byebug

  end

  test 'belongs_to schema' do
    father = IssueSchemaFather.create title: 'father'
    child = IssueSchemaChild.create title: 'child', issue_schema_father: father
    child.save
    assert_equal father, IssueSchemaChild.find(child.id).issue_schema_father
  end


end
