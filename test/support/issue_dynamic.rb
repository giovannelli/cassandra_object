class IssueDynamic < CassandraObject::BaseSchemalessDynamic
  string :description
  string :title

  before_create { self.description ||= 'funny' }

  self.allow_filtering = true

  def self.for_key key
    where_ids(key)
  end
end
