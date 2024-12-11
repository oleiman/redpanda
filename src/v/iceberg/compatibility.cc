// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/compatibility.h"

#include "iceberg/datatypes.h"
#include "iceberg/field_collecting_visitor.h"

#include <absl/container/btree_map.h>
#include <fmt/format.h>

#include <iostream>
#include <ranges>
#include <variant>

namespace iceberg {

std::ostream& operator<<(std::ostream& os, const evo_operation& op) {
    switch (op) {
    case evo_operation::reorder:
        return os << "REORDER";
    case evo_operation::update:
        return os << "UPDATE";
    case evo_operation::rename:
        return os << "RENAME";
    case evo_operation::add:
        return os << "ADD";
    case evo_operation::remove:
        return os << "REMOVE";
    case evo_operation::fill:
        return os << "FILL";
    }
}
std::ostream& operator<<(std::ostream& os, const evo_action& act) {
    if (act.src_field.has_value() && act.dest_field.has_value()) {
        fmt::print(
          os,
          "{{{} \n{} \n\t-> {}}}",
          act.op,
          **act.src_field,
          **act.dest_field);
    } else if (auto& f = act.src_field; f.has_value()) {
        fmt::print(os, "{{{} {}}}", act.op, **f);
    } else if (auto* f = act.dest_field.value_or(nullptr); f != nullptr) {
        fmt::print(os, "{{{} {}}}", act.op, *f);
    } // else {
    //     fmt::print(os, "{{{}}}", act.op);
    // }
    return os;
}
std::ostream& operator<<(std::ostream& os, const compat_plan& plan) {
    fmt::print(
      os,
      "plan: \n{}",
      fmt::join(
        plan.actions | std::views::filter([](const auto& act) {
            return act.op != evo_operation::fill;
        }),
        ",\n"));
    return os;
}

namespace {
struct primitive_type_promotion_policy_visitor {
    template<typename T, typename U>
    requires(!std::is_same_v<T, U>)
    bool operator()(const T&, const U&) const {
        return false;
    }

    template<typename T>
    bool operator()(const T&, const T&) const {
        return true;
    }

    // TODO(oren): simplifying assumption: assume any nested struct type is
    // compatible. Might make more sense to do like one layer of primitive type
    // checking?

    // template<typename T> bool operator()(
    //   const iceberg::struct_type& source, const iceberg::struct_type& dest) {
    //     chunked_vector<nested_field*> dest_stack;
    //     dest_stack.reserve(dest.fields.size());
    //     for (auto& f : std::ranges::reverse_view(dest.fields)) {
    //         dest_stack.emplace_back(f.get());
    //     }
    //     chunked_vector<nested_field*> source_stack;
    //     source_stack.reserve(source.fields.size());
    //     for (auto& f : std::ranges::reverse_view(source.fields)) {
    //         source_stack.emplace_back(f.get());
    //     }
    //     while (!source_stack.empty() && !dest_stack.empty()) {

    //     }
    // }

    bool operator()(const iceberg::int_type&, const iceberg::long_type&) const {
        return true;
    }

    bool operator()(
      const iceberg::date_type&, const iceberg::timestamp_type&) const {
        // TODO(oren): I think a bounds check is required here?
        return true;
    }

    // NOTE(oren): looks like timetsamp_ns is not supported. Intentional?
    // bool
    // operator()(const iceberg::date_type&, const iceberg::timestamp_ns_type)
    // const {
    //     return true;
    // }

    bool operator()(const iceberg::float_type&, const iceberg::double_type&) {
        return true;
    }

    bool operator()(
      const iceberg::decimal_type& src, const iceberg::decimal_type& dst) {
        return iceberg::primitive_type{src} == iceberg::primitive_type{dst}
               || (dst.scale == src.scale && dst.precision > src.precision);
    }

    bool
    operator()(const iceberg::fixed_type& src, const iceberg::fixed_type& dst) {
        return iceberg::primitive_type{src} == iceberg::primitive_type{dst};
    }
};

struct is_primitive_type_promotion_visitor {
    template<typename T, typename U>
    requires(!std::is_same_v<T, U>)
    bool operator()(const T&, const U&) const {
        return true;
    }

    template<typename T>
    bool operator()(const T&, const T&) const {
        return false;
    }

    bool operator()(
      const iceberg::decimal_type& src, const iceberg::decimal_type& dst) {
        return iceberg::primitive_type{src} != iceberg::primitive_type{dst};
    }
};

template<typename TypeCompatibilityVisitor>
struct type_promotion_visitor {
    explicit type_promotion_visitor(TypeCompatibilityVisitor vis)
      : vis_(std::move(vis)) {}
    template<typename T, typename U>
    requires(!std::is_same_v<T, U>)
    bool operator()(const T&, const U&) const {
        return false;
    }

    template<typename T>
    bool operator()(const T& src, const T& dst) const {
        // TODO(oren): is that allowed? just want to fwd straight through to the
        // visitor's "same type" semantics
        return vis_(src, dst);
    }

    bool operator()(
      const iceberg::primitive_type& src, const iceberg::primitive_type& dst) {
        return std::visit(vis_, src, dst);
    }

private:
    TypeCompatibilityVisitor vis_;
};
} // namespace

bool is_primitive_type_promotion(
  const iceberg::field_type& src, const iceberg::field_type& dst) {
    return std::visit(
      type_promotion_visitor{is_primitive_type_promotion_visitor{}}, src, dst);
}

bool satisfies_type_promotion_policy(
  const iceberg::field_type& src, const iceberg::field_type& dst) {
    return std::visit(
      type_promotion_visitor{primitive_type_promotion_policy_visitor{}},
      src,
      dst);
}

class dispatching_compat_visitor;
class schema_visitor;

struct post_visit {
    field_type* type;
    dispatching_compat_visitor* visitor;
    compat_plan get();
};

struct field_post_visit {
    field_post_visit(
      dispatching_compat_visitor* d, schema_visitor* v, nested_field* f)
      : vis(v)
      , field(f)
      , fut(&field->type, d) {}
    schema_visitor* vis;
    nested_field* field;
    post_visit fut;
    compat_plan get();
};

class schema_visitor {
public:
    explicit schema_visitor(const struct_type& source)
      : source_(source)
      , current_type_(source_.copy()) {}

    compat_plan
    operator()(struct_type&, chunked_vector<field_post_visit> fields) {
        vassert(
          std::holds_alternative<struct_type>(current_type_),
          "curr type must be struct");
        compat_plan result{};
        for (auto& fv : fields) {
            result.merge(fv.get());
        }
        return result;
    }

    compat_plan operator()(nested_field* f, post_visit& fut) {
        fmt::print(std::cerr, "VISITING NESTED FIELD {}\n", *f);
        compat_plan result{};

        static constexpr auto is_legal_req_update = [](
                                                      field_required src,
                                                      field_required dest) {
            return !(src == field_required::no && dest == field_required::yes);
        };

        auto find_match = [&f](const struct_type& s, bool require_name = true)
          -> std::optional<nested_field*> {
            for (const auto& sf : s.fields) {
                if (
                  (f->name == sf->name || !require_name)
                  && is_legal_req_update(sf->required, f->required)
                  && satisfies_type_promotion_policy(sf->type, f->type)) {
                    return sf.get();
                }
            }
            return std::nullopt;
        };

        auto strct = std::get<struct_type>(current_type_).copy();

        auto* sf = find_match(strct).value_or(
          find_match(strct, false).value_or(nullptr));

        if (sf != nullptr) {
            // TODO(oren): should be FILL
            result.actions.emplace_back(evo_operation::update, sf, f);
            std::visit(set_curr_type_visitor{current_type_}, sf->type);
            result.merge(fut.get());
        } else {
            result.actions.emplace_back(evo_operation::add, std::nullopt, f);
        }

        current_type_ = std::move(strct);

        return result;
    }

    compat_plan operator()(const list_type& t, post_visit fut) {
        vassert(
          std::holds_alternative<list_type>(current_type_),
          "Must be visiting a list");

        std::cerr << "VISIT LIST TYPE" << std::endl;

        auto list = std::get<list_type>(current_type_).copy();

        std::visit(
          set_curr_type_visitor{current_type_}, list.element_field->type);

        // TODO(oren): check element required

        auto element_plan = fut.get();

        // TODO(oren): are there rules on what kinds of evolutions can take
        // place for a list element?

        // TODO(oren): we'll probably be propagating errors, ultimately
        if (!element_plan.actions.empty()) {
            // make sure we assign the compatible source list's
            element_plan.actions.emplace_back(
              evo_operation::update,
              list.element_field.get(),
              t.element_field.get());
        }

        current_type_ = std::move(list);

        return element_plan;
    }

    compat_plan
    operator()(map_type& t, post_visit key_fut, post_visit val_fut) {
        vassert(
          std::holds_alternative<map_type>(current_type_), "Expected Map");

        auto map = std::get<map_type>(current_type_).copy();

        // TODO(oren): check key/value requiredness

        std::visit(set_curr_type_visitor{current_type_}, map.key_field->type);
        auto key_plan = key_fut.get();

        if (!key_plan.actions.empty()) {
            fmt::print(std::cerr, "Key plan: {}", key_plan);
            key_plan.actions.emplace_back(
              evo_operation::update, map.key_field.get(), t.key_field.get());
        }

        // TODO(oren): key update rules

        std::visit(set_curr_type_visitor{current_type_}, map.value_field->type);
        auto val_plan = val_fut.get();
        if (!val_plan.actions.empty()) {
            fmt::print(std::cerr, "Val plan: {}", val_plan);
            val_plan.actions.emplace_back(
              evo_operation::update,
              map.value_field.get(),
              t.value_field.get());
        }

        // TODO(oren): value update rules
        current_type_ = std::move(map);

        return std::move(key_plan.merge(std::move(val_plan)));
    }

    compat_plan operator()(const primitive_type& t) {
        vassert(
          std::holds_alternative<primitive_type>(current_type_),
          "Must be primitive (these shouldn't be assertions)");

        auto ptype = std::get<primitive_type>(current_type_);

        compat_plan result{};

        if (satisfies_type_promotion_policy(ptype, t)) {
            std::cerr << "ELEMENT TYPE MATCH" << std::endl;
            result.actions.emplace_back(
              evo_operation::update, std::nullopt, std::nullopt);
        }

        return result;
    }

private:
    // TODO(oren): collapse
    struct set_curr_type_visitor {
        field_type& curr;
        void operator()(const struct_type& t) { curr = t.copy(); }
        void operator()(const primitive_type& t) { curr = t; }
        void operator()(const list_type& t) { curr = t.copy(); }
        // TODO(oren): how do we set these???
        void operator()(const map_type& t) { curr = t.copy(); }
    };

    [[maybe_unused]] const struct_type& source_;
    field_type current_type_;
};

class dispatching_compat_visitor {
public:
    explicit dispatching_compat_visitor(const schema& source)
      : source_(source)
      , schema_vis_(source_.schema_struct) {}

    compat_plan operator()(struct_type& t) {
        compat_plan result{
          .actions = {},
          .source_highest_id = source_.highest_field_id().value_or(
            nested_field::id_t{0}),
        };

        chunked_vector<field_post_visit> fields;
        fields.reserve(t.fields.size());
        std::transform(
          t.fields.begin(),
          t.fields.end(),
          std::back_inserter(fields),
          [this](auto& f) {
              return field_post_visit{this, &schema_vis_, f.get()};
          });

        std::cerr << "Get ready for " << fields.size() << std::endl;

        result.merge(schema_vis_(t, std::move(fields)));

        return result;
    }

    compat_plan operator()(list_type& t) {
        return schema_vis_(
          t, post_visit{.type = &t.element_field->type, .visitor = this});

        // deferred_visit{.type = &t.element_field->type, .visitor = this}
    }

    compat_plan operator()(map_type& t) {
        return schema_vis_(
          t,
          post_visit{.type = &t.key_field->type, .visitor = this},
          post_visit{.type = &t.value_field->type, .visitor = this});
    }

    // TODO(oren): these all should return a checked result. then we can surface
    // incompatibilities and propagate them
    compat_plan operator()(const primitive_type& t) { return schema_vis_(t); }

    template<typename T>
    compat_plan operator()(const T&) {
        return compat_plan{};
    }

private:
    const schema& source_;
    schema_visitor schema_vis_;
};

compat_plan field_post_visit::get() { return (*vis)(field, fut); }
compat_plan post_visit::get() { return std::visit(*visitor, *type); }

compat_plan fancy_check_compatible(struct_type& dest, const schema& source) {
    return dispatching_compat_visitor{source}(dest);
}

struct ordinal_field {
    int ordinal;
    nested_field* field;
    template<typename H>
    friend H AbslHashValue(H h, const ordinal_field& e) {
        return H::combine(std::move(h), e.ordinal);
    }
};

// TODO(oren): don't really need an error code here. you should always be able
// to make a plan for converting one schema to another. trivially, just make all
// the original fields optional. and add all the new fields as new columns. the
// policy for whether a conversion plan is actually VALID can be applied later.
// We could do it all right here, but it will be easier to unit test an
// intermediate plan representation independent of the rest of the
// infrastructure classes and whatnot
checked<compat_plan, compat_errc>
is_compatible(struct_type& dest, const schema& source) {
    compat_plan plan{
      .actions = {},
      .source_highest_id = source.highest_field_id().value_or(
        nested_field::id_t{0}),
    };

    static constexpr auto cmp = [](const auto& a, const auto& b) {
        return a.ordinal < b.ordinal;
    };

    using ordinal_field_set_t = absl::btree_set<ordinal_field, decltype(cmp)>;

    // Traverse the destination type, assigning field IDs monotonically
    auto collect_ordinal_fields =
      [](const struct_type& type) -> ordinal_field_set_t {
        ordinal_field_set_t fields{cmp};
        int ord{1};
        chunked_vector<nested_field*> to_visit;
        for (auto& f : std::ranges::reverse_view(type.fields)) {
            to_visit.emplace_back(f.get());
        }
        while (!to_visit.empty()) {
            auto* f = to_visit.back();
            // only set if the field doesn't have a real ID
            fields.emplace(ord++, f);
            to_visit.pop_back();
            std::visit(reverse_field_collecting_visitor{to_visit}, f->type);
        }
        return fields;
    };

    auto dest_fields = collect_ordinal_fields(dest);

    auto source_fields = collect_ordinal_fields(source.schema_struct);

    // absl::btree_map<nested_field::id_t, std::optional<nested_field*>>
    //   source_fields;
    // for (auto& f : source.schema_struct.fields) {
    //     source_fields.emplace(f->id, f.get());
    // }

    static constexpr auto is_legal_req_update =
      [](field_required src, field_required dest) {
          return !(src == field_required::no && dest == field_required::yes);
      };

    // TODO(oren): these are gonna be way way expensive as formulated
    auto find_name_type_match =
      [&dest_fields](const nested_field* sf) -> std::optional<ordinal_field> {
        for (auto& [ord, df] : dest_fields) {
            if (
              df->name == sf->name
              && is_legal_req_update(sf->required, df->required)
              && satisfies_type_promotion_policy(sf->type, df->type)) {
                return ordinal_field{.ordinal = ord, .field = df};
            }
        }
        return std::nullopt;
    };

    auto find_type_match =
      [&dest_fields](const nested_field* sf) -> std::optional<ordinal_field> {
        for (auto& [ord, df] : dest_fields) {
            if (
              is_legal_req_update(sf->required, df->required)
              && satisfies_type_promotion_policy(sf->type, df->type)) {
                return ordinal_field{.ordinal = ord, .field = df};
            }
        }
        return std::nullopt;
    };

    ordinal_field_set_t skip_source_fields{cmp};

    for (const auto [ord, sf] : source_fields) {
        // NOTE(oren): worth nothing that the name and type matching (i.e. REUSE
        // FIELD ID) logic only really applies to a single nesting leve. since
        // it's illegal to move a field INTO or OUT OF a struct. that probably
        // simplifies things, really. We still need to make sure that two nested
        // structs are compatible, so there's a recursion there. and we need to
        // propagate the change set back out. but maybe we can just represent
        // the nesting in the translation plan or w/e.
        // NOTE(oren): this conditional encompasses all cases where a field ID
        // should be translated straight across from the source struct to the
        // dest struct. Not really sure whether there's any difference between
        // the two, but the assumption is that we want to try for name-matching
        // columns first. Not sure it means much of anything tbh.
        if (auto ntm = find_name_type_match(sf); ntm.has_value()) {
            // TODO(oren): reorder vs promote might not be a meaningful
            // distinction, but it could be useful for logging or might be
            // needed to push changes down into catalog
            auto ord_f = ntm.value();
            auto* df = ord_f.field;
            if (
              is_primitive_type_promotion(sf->type, df->type)
              || sf->required != df->required) {
                // TODO(oren): might not really have to do anything here
                plan.actions.emplace_back(evo_operation::update, sf, df);
            }
            if (ord_f.ordinal != ord) {
                plan.actions.emplace_back(evo_operation::reorder, sf, df);
            } else {
                plan.actions.emplace_back(evo_operation::fill, sf, df);
            }
            // otherwise we don't need to take any action,
            dest_fields.erase(ord_f);
            skip_source_fields.emplace(ord, sf);
        } else if (auto tm = find_type_match(sf); tm.has_value()) {
            auto ord_f = tm.value();
            auto* df = ord_f.field;
            plan.actions.emplace_back(evo_operation::rename, sf, df);
            dest_fields.erase(ord_f);
            skip_source_fields.emplace(ord, sf);
        }
    }

    for (const auto& [_, f] : dest_fields) {
        plan.actions.emplace_back(
          evo_operation::add, std::nullopt /*source*/, f /*dest*/);
    }

    for (auto& ord_f : source_fields) {
        if (!skip_source_fields.contains(ord_f)) {
            plan.actions.emplace_back(
              evo_operation::remove,
              ord_f.field /*source*/,
              std::nullopt /*dest*/);
        }
    }

    return plan;
}

checked<std::nullopt_t, compat_errc>
apply(struct_type& dest, compat_plan& plan) {
    auto free_id = plan.source_highest_id + 1;
    for (auto& action : plan.actions) {
        switch (action.op) {
        case evo_operation::rename:
        case evo_operation::update:
        case evo_operation::reorder:
        case evo_operation::fill:
            vassert(
              action.dest_field.has_value() && action.src_field.has_value(),
              "These ops need both source & dest");
            action.dest_field.value()->id = action.src_field.value()->id;
            break;
        case evo_operation::add:
            vassert(action.dest_field.has_value(), "These ops need dest");
            if (action.dest_field.value()->required) {
                return compat_errc::incompatible;
            }
            action.dest_field.value()->id = free_id++;
            break;
        case evo_operation::remove:
            vassert(action.src_field.has_value(), "These ops need source");
            dest.fields.push_back(action.src_field.value()->copy());
            dest.fields.back()->required = field_required::no;
            break;
        }
    }
    return std::nullopt;
}
} // namespace iceberg
