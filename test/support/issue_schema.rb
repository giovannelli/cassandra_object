class IssueSchema < CassandraObject::Base
  string :description
  string :title
  string :field

  before_create { self.description ||= 'funny' }

  def self.for_key key
    where_ids(key)
  end
end
