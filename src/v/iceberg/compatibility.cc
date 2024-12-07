// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/compatibility.h"

#include <variant>

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

template<typename PrimitiveVisitor>
struct type_promotion_visitor {
    explicit type_promotion_visitor(PrimitiveVisitor vis)
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
    PrimitiveVisitor vis_;
};

namespace iceberg {
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
} // namespace iceberg
