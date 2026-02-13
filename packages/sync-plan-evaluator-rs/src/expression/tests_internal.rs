    use super::*;
    use crate::model::RequestParameters;

    fn eval(expr: &SqlExpression) -> Value {
        evaluate_expression(
            expr,
            &EvalContext {
                row: None,
                request: &RequestParameters::default(),
            },
        )
        .unwrap()
    }

    fn int(value: i64) -> SqlExpression {
        SqlExpression::LitInt {
            base10: value.to_string(),
        }
    }

    fn double(value: f64) -> SqlExpression {
        SqlExpression::LitDouble { value }
    }

    #[test]
    fn integer_division_uses_integer_semantics() {
        let expr = SqlExpression::Binary {
            left: Box::new(int(7)),
            operator: "/".to_string(),
            right: Box::new(int(2)),
        };

        assert_eq!(eval(&expr), Value::Number(Number::from(3)));
    }

    #[test]
    fn real_division_keeps_fractional_result() {
        let expr = SqlExpression::Binary {
            left: Box::new(double(7.0)),
            operator: "/".to_string(),
            right: Box::new(int(2)),
        };

        assert_eq!(eval(&expr), Value::Number(Number::from_f64(3.5).unwrap()));
    }

    #[test]
    fn modulo_with_integer_inputs_stays_integer() {
        let expr = SqlExpression::Binary {
            left: Box::new(int(7)),
            operator: "%".to_string(),
            right: Box::new(int(3)),
        };

        assert_eq!(eval(&expr), Value::Number(Number::from(1)));
    }

    #[test]
    fn division_by_zero_returns_null() {
        let expr = SqlExpression::Binary {
            left: Box::new(int(7)),
            operator: "/".to_string(),
            right: Box::new(int(0)),
        };

        assert_eq!(eval(&expr), Value::Null);
    }

    #[test]
    fn text_and_numeric_equality_follow_sqlite_type_order() {
        let expr = SqlExpression::Binary {
            left: Box::new(SqlExpression::LitString {
                value: "1".to_string(),
            }),
            operator: "=".to_string(),
            right: Box::new(int(1)),
        };

        assert_eq!(eval(&expr), Value::Number(Number::from(0)));
    }

    #[test]
    fn not_equal_operator_is_supported() {
        let expr = SqlExpression::Binary {
            left: Box::new(SqlExpression::LitString {
                value: "1".to_string(),
            }),
            operator: "!=".to_string(),
            right: Box::new(int(1)),
        };

        assert_eq!(eval(&expr), Value::Number(Number::from(1)));
    }
