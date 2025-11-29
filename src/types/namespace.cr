module Kubernetes
  struct Namespace
    include Serializable

    field api_version : String = "v1"
    field kind : String = "Namespace"
    field metadata : Metadata

    struct Spec
      include Serializable

      field finalizers : Array(String) { %w[kubernetes] }
    end

    struct Status
      include Serializable

      field conditions : Array(Condition) { [] of Condition }
      field phase : String = ""
    end

      struct Condition
        include Serializable

        field type : String
        field status : String
        field last_transition_time : Time
        field reason : String
        field message : String
      end
  end

  define_resource "namespaces",
    group: "",
    type: Namespace
end
