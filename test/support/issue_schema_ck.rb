class IssueSchemaCk < CassandraObject::BaseSchema
  string :id
  string :type
  time :date
  float :value

  self.keys = '(id, type, date)'

  def self.for_key key
    where_ids(key)
  end
end
