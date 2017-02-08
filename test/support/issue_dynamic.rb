class IssueDynamic < CassandraObject::Base
  string :description
  string :title

  before_create { self.description ||= 'funny' }

  self.allow_filtering = true
  self.schema_type = :dynamic_attributes

  def self.for_key key
    where_ids(key)
  end
end
