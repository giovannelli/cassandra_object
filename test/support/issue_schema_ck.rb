class IssueSchemaCk < CassandraObject::BaseSchema
  string :id
  string :type
  time :date
  float :value

  self.allow_filtering = true

  self.keys = '(id, type, date)'

  def self.timestamps
    false
  end

  def self.for_key key
    where_ids(key)
  end
end
