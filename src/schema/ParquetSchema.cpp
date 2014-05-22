#include "schema/ParquetSchema.hpp"
#include "util/StringUtil.hpp"

namespace parquetbase {
namespace schema {



Element* generateSchemaElement(ThriftSchema::const_iterator& it, Element* parent) {
	ThriftSchemaElement el = *it;
	uint8_t d_level, r_level;
	if (parent == nullptr) {d_level = 0; r_level = 0;
	} else { d_level = parent->d_level; r_level = parent->r_level; }
	RepetitionType rtype = map(el.repetition_type);
	switch (rtype) {
	case RepetitionType::REQUIRED: break;
	case RepetitionType::OPTIONAL: d_level++; break;
	case RepetitionType::REPEATED: d_level++; r_level++; break;
	}
	if (el.num_children > 0) {
		GroupElement* cur = new GroupElement(el.name, rtype, parent, r_level, d_level);
		for (int i=0; i < el.num_children; i++) {
			cur->elements.push_back(generateSchemaElement(++it, cur));
		}
		return cur;
	} else
		return new SimpleElement(el.name, map(el.type), el.type_length, rtype, parent, r_level, d_level);
}


Element* generateSchema(ThriftSchema& schema) {
	ThriftSchema::const_iterator it = schema.cbegin();
	Element* s = generateSchemaElement(it, nullptr);
	return s;
}


void generateThriftSchemaInner(ThriftSchema& schemavec, GroupElement* schema, bool root=false) {
	ThriftSchemaElement s;
	s.__set_name(schema->name);
	s.__set_num_children(schema->elements.size());
	if (!root) s.__set_repetition_type(unmap(schema->repetition));
	schemavec.push_back(s);
	for (auto* el : schema->elements) {
		if (dynamic_cast<schema::GroupElement*>(el) != nullptr) {
			generateThriftSchemaInner(schemavec, dynamic_cast<GroupElement*>(el));
		} else {
			SimpleElement* sel = dynamic_cast<SimpleElement*>(el);
			ThriftSchemaElement s;
			s.__set_name(sel->name);
			s.__set_repetition_type(unmap(sel->repetition));
			s.__set_type(schema::unmap(sel->type));
			schemavec.push_back(s);
		}
	}
}


ThriftSchema generateThriftSchema(GroupElement* schema) {
	ThriftSchema schemavec;
	generateThriftSchemaInner(schemavec, schema, true);
	return schemavec;
}


Element* GroupElement::navigate(std::string basename, char separator) {
	if (basename == "") return this;
	std::vector<std::string> path = parquetbase::util::split(basename, separator);
	auto it = path.begin();
	auto end = path.end();
	return navigate(it, end);
}


Element* GroupElement::navigate(std::vector<std::string>::iterator& it, std::vector<std::string>::iterator& end) {
	std::string name = *it;
	++it;
	for (auto* el : elements) {
		if (el->name == name) {
			if (it == end) return el;
			else return dynamic_cast<GroupElement*>(el)->navigate(it, end);
		}
	}
	return nullptr;
}


}}
