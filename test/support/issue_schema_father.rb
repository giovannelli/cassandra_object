class IssueSchemaFather < CassandraObject::BaseSchema
  string :id
  string :title
  float :field

  validates :title, presence: true

  self.keys = '(id)'

  def self.for_key key
    where_ids(key)
  end
end
