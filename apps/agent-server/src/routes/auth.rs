use crate::{ApiError, AppState};
use axum::{
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::Response,
};
use subtle::ConstantTimeEq;

pub async fn authenticate(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let presented = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .and_then(|(scheme, token)| scheme.eq_ignore_ascii_case("bearer").then_some(token));
    let authenticated = presented.is_some_and(|presented| {
        state.tokens.iter().fold(false, |matched, configured| {
            let equal = configured.len() == presented.len()
                && bool::from(configured.as_bytes().ct_eq(presented.as_bytes()));
            matched | equal
        })
    });
    if !authenticated {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "a valid bearer token is required",
        ));
    }
    Ok(next.run(request).await)
}
