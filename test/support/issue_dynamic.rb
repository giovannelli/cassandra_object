class IssueDynamic < CassandraObject::Base
  string :description
  string :title

  before_create { self.description ||= 'funny' }

  self.allow_filtering = true
  self.dynamic_attributes = true
  self.schemaless = true

  def self.for_key key
    where_ids(key)
  end
end
