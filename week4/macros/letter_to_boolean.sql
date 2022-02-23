{#
    This macro returns the true/false for Y/N 
#}

{% macro letter_to_boolean(string_field) -%}

    case {{ string_field }}
        when 'Y' then true
        when 'N' then false
    end

{%- endmacro %}