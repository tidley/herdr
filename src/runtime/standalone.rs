//! Adapter between the standalone socket API and the shared App turn lifecycle.
//!
//! It borrows the host App for one request. The standalone server retains
//! ownership of its sockets, clients, rendering, and UI state.

pub(crate) struct StandaloneRuntimeAdapter;

impl StandaloneRuntimeAdapter {
    pub(crate) fn claims(&self, request: &crate::api::schema::Request) -> bool {
        matches!(
            request.method,
            crate::api::schema::Method::AgentTurn(_)
                | crate::api::schema::Method::AgentInterrupt(_)
        )
    }

    pub(crate) fn dispatch(
        &mut self,
        app: &mut crate::app::App,
        request: crate::api::schema::Request,
    ) -> Option<String> {
        match request.method {
            crate::api::schema::Method::AgentTurn(params) => {
                Some(app.handle_runtime_agent_turn(request.id, params))
            }
            crate::api::schema::Method::AgentInterrupt(target) => {
                Some(app.handle_runtime_agent_interrupt(request.id, target))
            }
            _ => None,
        }
    }
}
