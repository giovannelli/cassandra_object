class IssueSchema < CassandraObject::BaseSchema
  string :id
  string :description
  string :title
  float :field
  integer :intero

  before_create { self.description ||= 'funny' }

  validates :title, presence: true

  self.keys = '(id)'

  def self.for_key key
    where_ids(key)
  end
end
