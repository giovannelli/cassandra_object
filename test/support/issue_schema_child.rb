class IssueSchemaChild < CassandraObject::BaseSchema
  string :id
  string :description
  string :title
  float :field

  belongs_to :issue_schema_father
  string :issue_schema_father_id

  validates :title, presence: true

  self.keys = '(id)'

  def self.for_key key
    where_ids(key)
  end
end
