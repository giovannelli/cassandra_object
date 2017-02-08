class IssueSchema < CassandraObject::Base
  string :id
  string :description
  string :title
  float :field
  integer :intero
  time :created_at
  time :updated_at

  before_create { self.description ||= 'funny' }

  validates :title, presence: true

  self.keys = '(id)'

  def self.for_key key
    where_ids(key)
  end
end
