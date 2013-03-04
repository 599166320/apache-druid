grammar DruidSQL;

@header {
import com.metamx.druid.aggregation.post.*;
import com.metamx.druid.aggregation.*;
import com.metamx.druid.query.filter.*;
import com.metamx.druid.query.dimension.*;
import com.metamx.druid.*;

import com.google.common.base.*;
import com.google.common.collect.Lists;
import org.joda.time.*;

import java.text.*;
import java.util.*;
}

@parser::members {
    public Map<String, AggregatorFactory> aggregators = new LinkedHashMap<String, AggregatorFactory>();
    public List<PostAggregator> postAggregators = new LinkedList<PostAggregator>();
    public DimFilter filter;
    public List<org.joda.time.Interval> intervals;
    public QueryGranularity granularity = QueryGranularity.ALL;
    public Map<String, DimensionSpec> groupByDimensions = new LinkedHashMap<String, DimensionSpec>();
    
    String dataSourceName = null;
    
    public String getDataSource() {
        return dataSourceName;
    }
    
    public String unescape(String quoted) {
        String unquote = quoted.trim().replaceFirst("^'(.*)'\$", "\$1");
        return unquote.replace("''", "'");
    }

    AggregatorFactory evalAgg(String name, int fn) {
        switch (fn) {
            case SUM: return new DoubleSumAggregatorFactory("sum("+name+")", name);
            case MIN: return new MinAggregatorFactory("min("+name+")", name);
            case MAX: return new MaxAggregatorFactory("max("+name+")", name);
            case COUNT: return new CountAggregatorFactory(name);
        }
        throw new IllegalArgumentException("Unknown function [" + fn + "]"); 
    }
    
    PostAggregator evalArithmeticPostAggregator(PostAggregator a, List<Token> ops, List<PostAggregator> b) {
        if(b.isEmpty()) return a;
        else {            
            int i = 0;
            
            PostAggregator root = a;
            while(i < ops.size()) {
                List<PostAggregator> list = new LinkedList<PostAggregator>();
                List<String> names = new LinkedList<String>();

                names.add(root.getName());
                list.add(root);
                
                Token op = ops.get(i);
                
                while(i < ops.size() && ops.get(i).getType() == op.getType()) {
                    PostAggregator e = b.get(i);
                    list.add(e);
                    names.add(e.getName());
                    i++;
                }
                
                root = new ArithmeticPostAggregator("("+Joiner.on(op.getText()).join(names)+")", op.getText(), list);
            }
            
            return root;
        }
    }
}


AND: 'and';
OR: 'or';
SUM: 'sum';
MIN: 'min';
MAX: 'max';
COUNT: 'count';
AS: 'as';
OPEN: '(';
CLOSE: ')';
STAR: '*';
NOT: '!' ;
PLUS: '+';
MINUS: '-';
DIV: '/';
COMMA: ',';
GROUP: 'group';

IDENT : (LETTER)(LETTER | DIGIT | '_')* ;
QUOTED_STRING : '\'' ( ESC | ~'\'' )*? '\'' ;
ESC : '\'' '\'';

NUMBER: ('+'|'-')?DIGIT*'.'?DIGIT+(EXPONENT)?;
EXPONENT: ('e') ('+'|'-')? ('0'..'9')+;
fragment DIGIT : '0'..'9';
fragment LETTER : 'a'..'z' | 'A'..'Z';

LINE_COMMENT : '--' .*? '\r'? '\n' -> skip ;
COMMENT      : '/*' .*? '*/' -> skip ;
WS :  (' '| '\t' | '\r' '\n' | '\n' | '\r')+ -> skip;



query
    : select_stmt where_stmt (groupby_stmt)?
    ;

select_stmt
    : 'select' e+=aliasedExpression (',' e+=aliasedExpression)* 'from' datasource {
        for(AliasedExpressionContext a : $e) { postAggregators.add(a.p); }
        this.dataSourceName = $datasource.text;
    }
    ;

where_stmt
    : 'where' f=timeAndDimFilter {
        if($f.filter != null) this.filter = $f.filter;
        this.intervals = Lists.newArrayList($f.interval);
    }
    ;

groupby_stmt
    : GROUP 'by' groupByExpression ( COMMA! groupByExpression )*
    ;

groupByExpression
    : gran=granularityFn {this.granularity = $gran.granularity;}
    | dim=IDENT { this.groupByDimensions.put($dim.text, new DefaultDimensionSpec($dim.text, $dim.text)); }
    ;

datasource
    : IDENT 
    ;

aliasedExpression returns [PostAggregator p]
    : expression ( AS^ name=IDENT )? {
        if($name != null) {
            postAggregators.add($expression.p);
            $p = new FieldAccessPostAggregator($name.text, $expression.p.getName());
        }
        else $p = $expression.p;
    }
    ;

expression returns [PostAggregator p]
    : additiveExpression { $p = $additiveExpression.p; }
    ;

additiveExpression returns [PostAggregator p]
    : a=multiplyExpression (( ops+=PLUS^ | ops+=MINUS^ ) b+=multiplyExpression)* {
        List<PostAggregator> rhs = new LinkedList<PostAggregator>();
        for(MultiplyExpressionContext e : $b) rhs.add(e.p);
        $p = evalArithmeticPostAggregator($a.p, $ops, rhs);
    }
    ;

multiplyExpression returns [PostAggregator p]
    : a=unaryExpression ((ops+= STAR | ops+=DIV ) b+=unaryExpression)* {
        List<PostAggregator> rhs = new LinkedList<PostAggregator>();
        for(UnaryExpressionContext e : $b) rhs.add(e.p);
        $p = evalArithmeticPostAggregator($a.p, $ops, rhs);
    }
    ;

unaryExpression returns [PostAggregator p]
    : MINUS e=unaryExpression {
        $p = new ArithmeticPostAggregator(
            "-"+$e.p.getName(),
            "*",
            Lists.newArrayList($e.p, new ConstantPostAggregator("-1", -1.0))
        );
    }
    | PLUS e=unaryExpression { $p = $e.p; }
    | primaryExpression { $p = $primaryExpression.p; }
    ;

primaryExpression returns [PostAggregator p]
    : constant { $p = $constant.c; }
    | aggregate {
        aggregators.put($aggregate.agg.getName(), $aggregate.agg);
        $p = new FieldAccessPostAggregator($aggregate.agg.getName(), $aggregate.agg.getName());
    }
    | OPEN! e=expression CLOSE! { $p = $e.p; }
    ;

aggregate returns [AggregatorFactory agg]
    : fn=( SUM^ | MIN^ | MAX^ ) OPEN! name=(IDENT|COUNT) CLOSE! { $agg = evalAgg($name.text, $fn.type); }
      | fn=COUNT OPEN! STAR CLOSE! { $agg = evalAgg("count(*)", $fn.type); }
    ;

constant returns [ConstantPostAggregator c]
    : value=NUMBER  { double v = Double.parseDouble($value.text); $c = new ConstantPostAggregator(Double.toString(v), v); }
    ;

/* time filters must be top level filters */
timeAndDimFilter returns [DimFilter filter, org.joda.time.Interval interval]
    : t=timeFilter (AND f=dimFilter)?  {
        if($f.ctx != null) $filter = $f.filter;
        $interval = $t.interval;
    }
    | (f=dimFilter)? AND t=timeFilter {
        if($f.ctx != null) $filter = $f.filter;
        $interval = $t.interval;
    }
    ;

dimFilter returns [DimFilter filter]
    : e=orDimFilter { $filter = $e.filter; }
    ;

orDimFilter returns [DimFilter filter]
    : a=andDimFilter (OR^ b+=andDimFilter)* {
        if($b.isEmpty()) $filter = $a.filter;
        else {
            List<DimFilter> rest = new ArrayList<DimFilter>();
            for(AndDimFilterContext e : $b) rest.add(e.filter);
            $filter = new OrDimFilter(Lists.asList($a.filter, rest.toArray(new DimFilter[]{})));
        }
    }
    ;

andDimFilter returns [DimFilter filter]
    : a=primaryDimFilter (AND^ b+=primaryDimFilter)* {
        if($b.isEmpty()) $filter = $a.filter;
        else {
            List<DimFilter> rest = new ArrayList<DimFilter>();
            for(PrimaryDimFilterContext e : $b) rest.add(e.filter);
            $filter = new AndDimFilter(Lists.asList($a.filter, rest.toArray(new DimFilter[]{})));
        }
    }
    ;

primaryDimFilter returns [DimFilter filter]
    : e=selectorDimFilter { $filter = $e.filter; }
    | NOT f=dimFilter { $filter = new NotDimFilter($f.filter); }
    | OPEN! f=dimFilter CLOSE! { $filter = $f.filter; }
    ;

selectorDimFilter returns [SelectorDimFilter filter]
    : dimension=IDENT '=' value=QUOTED_STRING {
        $filter = new SelectorDimFilter($dimension.text, unescape($value.text));
    }
    ;

timeFilter returns [org.joda.time.Interval interval, QueryGranularity granularity]
    : 'timestamp' 'between' s=timestamp AND e=timestamp {
        $interval = new org.joda.time.Interval($s.t, $e.t);
    }
    ;

granularityFn returns [QueryGranularity granularity]
    : 'granularity' OPEN! 'timestamp' ',' str=QUOTED_STRING CLOSE! {
        String granStr = unescape($str.text);
        try {
            $granularity = QueryGranularity.fromString(granStr);
        } catch(IllegalArgumentException e) {
            $granularity = new PeriodGranularity(new Period(granStr), null, null);
        }
    }
    ;

timestamp returns [DateTime t]
    : NUMBER {
        String str = $NUMBER.text.trim();
        try {
            $t = new DateTime(NumberFormat.getInstance().parse(str));
        }
        catch(ParseException e) {
            throw new IllegalArgumentException("Unable to parse number [" + str + "]");
        }
    }
    | QUOTED_STRING { $t = new DateTime(unescape($QUOTED_STRING.text)); }
    ;
