//! Responsive layout engine and transcript geometry resolver.

use std::sync::Arc;

use ratatui::layout::Rect;

use crate::tui_v2::{
    app::route::AppRoute,
    input::action::ActionId,
    model::{
        focus::{ControlId, FocusTarget},
        overlay::OverlayState,
    },
};

use super::{
    anchor::AnchorMode,
    dock::{split_dock, DEFAULT_PLAN_RATIO},
    measure::MeasuredPart,
    responsive::{
        centered_overlay, centered_scrollbar_x, compose_route, transcript_column_with_dock,
        ResponsiveClass, SCROLLBAR_CONTENT_GAP,
    },
    snapshot::{
        intersect, transition_between, InteractionId, InteractionIntent, InteractionRegion,
        InvalidationPlan, LayoutRegion, LayoutRegionId, LayoutSnapshot, RouteLayoutKind,
        ScrollRegionId, TranscriptLayout,
    },
    transcript::layout_transcript,
};

pub struct TranscriptRequest<'a> {
    pub items: &'a [Arc<MeasuredPart>],
    pub spacing_before: &'a [u16],
    pub expandable: &'a [crate::tui_v2::model::artifact::PartId],
    pub anchor: AnchorMode,
    pub new_content_count: usize,
}

pub struct LayoutRequest<'a> {
    pub viewport: Rect,
    pub route: &'a AppRoute,
    pub overlay: Option<&'a OverlayState>,
    pub focus: &'a FocusTarget,
    pub inspector_requested: bool,
    /// Fraction of dock height for the plan band (0.2–0.8).
    pub dock_plan_ratio: f32,
    /// When true, plan band compresses so the plugin well can grow.
    pub plugin_focused: bool,
    pub fullscreen_artifact: Option<&'a crate::tui_v2::model::artifact::PartId>,
    pub decision_dock_height: u16,
    pub composer_content_rows: u16,
    /// Unclamped soft-wrap line count — used to decide if a scrollbar is needed.
    pub composer_total_rows: u16,
    pub composer_fullscreen: bool,
    pub composer_autocomplete_rows: u16,
    pub transcript: Option<TranscriptRequest<'a>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LayoutPass {
    pub snapshot: LayoutSnapshot,
    pub invalidation: InvalidationPlan,
}

#[derive(Debug, Default)]
pub struct LayoutEngine {
    generation: u64,
    previous: Option<LayoutSnapshot>,
}

impl LayoutEngine {
    pub fn layout(&mut self, request: LayoutRequest<'_>) -> LayoutPass {
        self.generation = self.generation.saturating_add(1);
        let class = ResponsiveClass::resolve(request.viewport);
        let route = route_kind(request.route);
        let mut regions = vec![region(
            LayoutRegionId::Canvas,
            request.viewport,
            request.viewport,
        )];
        let mut interactions = Vec::new();
        let mut transcript = TranscriptLayout {
            viewport: Rect::new(request.viewport.x, request.viewport.y, 0, 0),
            ..TranscriptLayout::default()
        };

        if let Some(geometry) = compose_route(
            request.viewport,
            request.inspector_requested,
            request.composer_content_rows,
        ) {
            regions.extend([
                region(
                    LayoutRegionId::ContextBar,
                    geometry.context_bar,
                    request.viewport,
                ),
                region(
                    LayoutRegionId::TopDivider,
                    geometry.top_divider,
                    request.viewport,
                ),
                region(LayoutRegionId::Primary, geometry.primary, request.viewport),
                region(
                    LayoutRegionId::Composer,
                    geometry.composer,
                    request.viewport,
                ),
                region(
                    LayoutRegionId::StatusLine,
                    geometry.status_line,
                    request.viewport,
                ),
                region(
                    LayoutRegionId::BottomDivider,
                    geometry.bottom_divider,
                    request.viewport,
                ),
            ]);
            // Three-band context bar: left chrome · empty center · right title.
            // Global working indicator is the bottom purple edge rail.
            let bar = geometry.context_bar;
            let center_w = if bar.width >= 36 {
                14u16.min(bar.width / 4).max(10)
            } else if bar.width >= 24 {
                10
            } else {
                0
            };
            let side = bar.width.saturating_sub(center_w) / 2;
            let right_w = bar.width.saturating_sub(side).saturating_sub(center_w);
            let context_identity = Rect::new(bar.x, bar.y, side, bar.height);
            let context_status = Rect::new(bar.x.saturating_add(side), bar.y, center_w, bar.height);
            let context_meta = Rect::new(
                bar.x.saturating_add(side).saturating_add(center_w),
                bar.y,
                right_w,
                bar.height,
            );
            regions.extend([
                region(
                    LayoutRegionId::ContextIdentity,
                    context_identity,
                    request.viewport,
                ),
                region(
                    LayoutRegionId::ContextStatus,
                    context_status,
                    request.viewport,
                ),
                region(LayoutRegionId::ContextMeta, context_meta, request.viewport),
            ]);
            // Conversation only: click title (right context meta) to rename.
            if matches!(request.route, AppRoute::Conversation { .. }) {
                interactions.push(InteractionRegion {
                    id: InteractionId::new("context-title"),
                    bounds: context_meta,
                    clip: intersect(context_meta, request.viewport),
                    intent: InteractionIntent::EditSessionTitle,
                });
            }
            // Full-bleed with the workspace: edge-to-edge under the primary +
            // side panel so the input bar shares the panel's outer right edge.
            // Fullscreen still floats a centered editor over the transcript.
            let composer_width = if request.composer_fullscreen {
                request.viewport.width.saturating_sub(4).min(100)
            } else {
                geometry.composer.width
            };
            let composer_field_height = if request.composer_fullscreen {
                geometry
                    .composer
                    .bottom()
                    .saturating_sub(geometry.primary.y)
            } else {
                request
                    .composer_content_rows
                    .clamp(1, 4)
                    .saturating_add(2)
                    .min(geometry.composer.height)
            };
            let composer_field = Rect::new(
                if request.composer_fullscreen {
                    request
                        .viewport
                        .x
                        .saturating_add(request.viewport.width.saturating_sub(composer_width) / 2)
                } else {
                    geometry.composer.x
                },
                if request.composer_fullscreen {
                    geometry.primary.y
                } else {
                    geometry.composer.y.saturating_add(
                        geometry
                            .composer
                            .height
                            .saturating_sub(composer_field_height)
                            / 2,
                    )
                },
                composer_width,
                composer_field_height,
            );
            regions.push(region(
                LayoutRegionId::ComposerField,
                composer_field,
                request.viewport,
            ));
            if request.composer_autocomplete_rows > 0 {
                // Body rows + titleless popup chrome (border top + bottom only).
                let body_rows = request.composer_autocomplete_rows.clamp(1, 12);
                let chrome =
                    crate::tui_v2::components::primitive::assist_chrome::ASSIST_CHROME_ROWS;
                let autocomplete_height = body_rows
                    .saturating_add(chrome)
                    .min(geometry.primary.height);
                let autocomplete = Rect::new(
                    composer_field.x,
                    geometry.primary.y.saturating_add(
                        geometry.primary.height.saturating_sub(autocomplete_height),
                    ),
                    composer_field.width,
                    autocomplete_height,
                );
                regions.push(region(
                    LayoutRegionId::ComposerAutocomplete,
                    autocomplete,
                    request.viewport,
                ));
                interactions.push(InteractionRegion {
                    id: InteractionId::new("composer-assist"),
                    bounds: autocomplete,
                    clip: intersect(autocomplete, request.viewport),
                    intent: InteractionIntent::ComposerAssist,
                });
                interactions.push(InteractionRegion {
                    id: InteractionId::new("composer-assist-scroll"),
                    bounds: autocomplete,
                    clip: intersect(autocomplete, request.viewport),
                    intent: InteractionIntent::Scroll(ScrollRegionId::ComposerAssist),
                });
            }
            let status_split = geometry.status_line.width.saturating_mul(3) / 5;
            regions.extend([
                region(
                    LayoutRegionId::StatusMeta,
                    Rect::new(
                        geometry.status_line.x,
                        geometry.status_line.y,
                        status_split,
                        geometry.status_line.height,
                    ),
                    request.viewport,
                ),
                region(
                    LayoutRegionId::ActionFooter,
                    Rect::new(
                        geometry.status_line.x.saturating_add(status_split),
                        geometry.status_line.y,
                        geometry.status_line.width.saturating_sub(status_split),
                        geometry.status_line.height,
                    ),
                    request.viewport,
                ),
            ]);
            if let Some(inspector) = geometry.inspector {
                regions.push(region(
                    LayoutRegionId::Inspector,
                    inspector,
                    request.viewport,
                ));
                let ratio = if request.dock_plan_ratio > 0.0 {
                    request.dock_plan_ratio
                } else {
                    DEFAULT_PLAN_RATIO
                };
                if let Some(dock) = split_dock(inspector, ratio, request.plugin_focused) {
                    regions.push(region(
                        LayoutRegionId::PlanDock,
                        dock.plan,
                        request.viewport,
                    ));
                    interactions.push(InteractionRegion {
                        id: InteractionId::new("plan-dock"),
                        bounds: dock.plan,
                        clip: intersect(dock.plan, request.viewport),
                        intent: InteractionIntent::Focus(FocusTarget::PlanDock),
                    });
                    if let Some(gap) = dock.gap {
                        regions.push(region(LayoutRegionId::DockDivider, gap, request.viewport));
                    }
                    if dock.plugin.height > 0 {
                        regions.push(region(
                            LayoutRegionId::PluginDock,
                            dock.plugin,
                            request.viewport,
                        ));
                        // Focus intent is above scroll so clicks focus; wheel still
                        // finds the scroll region by scanning Scroll intents.
                        interactions.push(InteractionRegion {
                            id: InteractionId::new("plugin-dock"),
                            bounds: dock.plugin,
                            clip: intersect(dock.plugin, request.viewport),
                            intent: InteractionIntent::Focus(FocusTarget::PluginDock),
                        });
                        interactions.push(InteractionRegion {
                            id: InteractionId::new("plugin-dock-scroll"),
                            bounds: dock.plugin,
                            clip: intersect(dock.plugin, request.viewport),
                            intent: InteractionIntent::Scroll(ScrollRegionId::PluginDock),
                        });
                    }
                    interactions.push(InteractionRegion {
                        id: InteractionId::new("plan-dock-scroll"),
                        bounds: dock.plan,
                        clip: intersect(dock.plan, request.viewport),
                        intent: InteractionIntent::Scroll(ScrollRegionId::PlanDock),
                    });
                }
            }

            // Scrollbar sits in the right border column but only spans the
            // *inner* content height so it does not run over top/bottom chrome.
            let inner_h = composer_field.height.saturating_sub(2).max(1);
            let composer_content = if request.composer_total_rows > inner_h
                && composer_field.width > 2
                && !request.composer_fullscreen
            {
                let sb = Rect::new(
                    composer_field.right().saturating_sub(1),
                    composer_field.y.saturating_add(1),
                    1,
                    inner_h,
                );
                regions.push(region(
                    LayoutRegionId::ComposerScrollbar,
                    sb,
                    request.viewport,
                ));
                interactions.push(InteractionRegion {
                    id: InteractionId::new("composer-scrollbar"),
                    bounds: sb,
                    clip: intersect(sb, request.viewport),
                    intent: InteractionIntent::Scrollbar(ScrollRegionId::Composer),
                });
                Rect::new(
                    composer_field.x,
                    composer_field.y,
                    composer_field.width.saturating_sub(1),
                    composer_field.height,
                )
            } else {
                composer_field
            };
            // Keep ComposerField region as the clickable/editable field (may be
            // slightly narrower when a scrollbar is reserved).
            if let Some(existing) = regions
                .iter_mut()
                .find(|region| region.id == LayoutRegionId::ComposerField)
            {
                existing.rect = composer_content;
                existing.clip = intersect(composer_content, request.viewport);
            }
            interactions.push(InteractionRegion {
                id: InteractionId::new("composer"),
                bounds: composer_content,
                clip: intersect(composer_content, request.viewport),
                intent: InteractionIntent::Focus(FocusTarget::Composer),
            });
            interactions.push(InteractionRegion {
                id: InteractionId::new("composer-scroll"),
                bounds: composer_content,
                clip: intersect(composer_content, request.viewport),
                intent: InteractionIntent::Scroll(ScrollRegionId::Composer),
            });

            let transcript_area = if request.decision_dock_height > 0 {
                let dock_height = geometry
                    .primary
                    .height
                    .saturating_sub(1)
                    .min(request.decision_dock_height);
                let dock = Rect::new(
                    geometry.primary.x,
                    geometry.primary.bottom().saturating_sub(dock_height),
                    geometry.primary.width,
                    dock_height,
                );
                regions.push(region(LayoutRegionId::DecisionDock, dock, request.viewport));
                interactions.push(InteractionRegion {
                    id: InteractionId::new("decision-dock"),
                    bounds: dock,
                    clip: intersect(dock, request.viewport),
                    intent: InteractionIntent::Focus(FocusTarget::DecisionDock),
                });
                if request.decision_dock_height <= 4 {
                    let button_widths = [11_u16, 8, 11];
                    let buttons_width = button_widths.into_iter().sum::<u16>();
                    let mut button_x = dock.right().saturating_sub(1).saturating_sub(buttons_width);
                    for (id, action, width) in [
                        (
                            LayoutRegionId::DecisionApprove,
                            ActionId::ApproveDecision,
                            button_widths[0],
                        ),
                        (
                            LayoutRegionId::DecisionDeny,
                            ActionId::DenyDecision,
                            button_widths[1],
                        ),
                        (
                            LayoutRegionId::DecisionInspect,
                            ActionId::InspectDecision,
                            button_widths[2],
                        ),
                    ] {
                        let button = Rect::new(
                            button_x,
                            dock.y.saturating_add(1),
                            width.min(dock.right().saturating_sub(button_x)),
                            dock.height.saturating_sub(2).min(1),
                        );
                        regions.push(region(id, button, request.viewport));
                        interactions.push(InteractionRegion {
                            id: InteractionId::new(format!("decision:{action:?}")),
                            bounds: button,
                            clip: intersect(button, request.viewport),
                            intent: InteractionIntent::Invoke(action),
                        });
                        button_x = button_x.saturating_add(width);
                    }
                }
                transcript_column_with_dock(
                    Rect::new(
                        geometry.primary.x,
                        geometry.primary.y,
                        geometry.primary.width,
                        geometry.primary.height.saturating_sub(dock_height),
                    ),
                    geometry.inspector.is_some(),
                )
            } else {
                transcript_column_with_dock(geometry.primary, geometry.inspector.is_some())
            };

            if let Some(transcript_request) = request.transcript {
                // With a dock: scrollbar is centered in the primary→inspector
                // channel (equal pad on both sides). Without a dock: reserve a
                // mirrored gutter at the right edge of the stream.
                let overflow = transcript_request
                    .items
                    .iter()
                    .map(|item| item.height())
                    .fold(0_u32, u32::saturating_add)
                    > u32::from(transcript_area.height);
                let (content_area, scrollbar) = if !overflow {
                    (transcript_area, None)
                } else if let Some(inspector) = geometry.inspector {
                    // Full dock height (plan + plugin), with 1-cell breath at the
                    // top and bottom so the rail does not smash the dock frame.
                    let channel_left = geometry.primary.right();
                    let channel_right = inspector.x;
                    let sb_x =
                        centered_scrollbar_x(channel_left, channel_right).unwrap_or(channel_left);
                    let inset = 1u16.min(inspector.height.saturating_sub(2) / 2);
                    let sb_y = inspector.y.saturating_add(inset);
                    let sb_h = inspector
                        .height
                        .saturating_sub(inset.saturating_mul(2))
                        .max(1);
                    (transcript_area, Some(Rect::new(sb_x, sb_y, 1, sb_h)))
                } else {
                    // No dock: pad · track with equal outer breath; height follows
                    // the full stream column with the same 1-cell end breath.
                    let reserve = 1u16.saturating_add(SCROLLBAR_CONTENT_GAP);
                    let inset = 1u16.min(geometry.primary.height.saturating_sub(2) / 2);
                    let track_y = geometry.primary.y.saturating_add(inset);
                    let track_h = geometry
                        .primary
                        .height
                        .saturating_sub(inset.saturating_mul(2))
                        .max(1);
                    if transcript_area.width > reserve.saturating_add(4) {
                        let sb = Rect::new(
                            transcript_area.right().saturating_sub(1),
                            track_y,
                            1,
                            track_h,
                        );
                        (
                            Rect::new(
                                transcript_area.x,
                                transcript_area.y,
                                transcript_area.width.saturating_sub(reserve),
                                transcript_area.height,
                            ),
                            Some(sb),
                        )
                    } else {
                        (transcript_area, None)
                    }
                };
                if let Some(sb) = scrollbar {
                    regions.push(region(
                        LayoutRegionId::TranscriptScrollbar,
                        sb,
                        request.viewport,
                    ));
                    interactions.push(InteractionRegion {
                        id: InteractionId::new("transcript-scrollbar"),
                        bounds: sb,
                        clip: intersect(sb, request.viewport),
                        intent: InteractionIntent::Scrollbar(ScrollRegionId::Transcript),
                    });
                }
                transcript = layout_transcript(
                    content_area,
                    transcript_request.items,
                    transcript_request.spacing_before,
                    &transcript_request.anchor,
                );
                regions.push(region(
                    LayoutRegionId::Transcript,
                    content_area,
                    request.viewport,
                ));
                interactions.push(InteractionRegion {
                    id: InteractionId::new("transcript-scroll"),
                    bounds: content_area,
                    clip: intersect(content_area, request.viewport),
                    intent: InteractionIntent::Scroll(ScrollRegionId::Transcript),
                });
                for part in &transcript.parts {
                    let intent = if transcript_request.expandable.contains(&part.part_id) {
                        InteractionIntent::ToggleArtifact(part.part_id.clone())
                    } else {
                        InteractionIntent::Focus(FocusTarget::Transcript {
                            part_id: part.part_id.clone(),
                        })
                    };
                    interactions.push(InteractionRegion {
                        id: InteractionId::new(format!("part:{}", part.part_id.as_str())),
                        bounds: part.visible_rect,
                        clip: intersect(part.visible_rect, transcript_area),
                        intent,
                    });
                    if let Some(measured) = transcript_request
                        .items
                        .iter()
                        .find(|measured| measured.key.part_id == part.part_id)
                    {
                        append_link_interactions(&mut interactions, measured, part, content_area);
                    }
                }
                if transcript_request.new_content_count > 0 && content_area.height > 0 {
                    let width = content_area.width.min(30);
                    let indicator = Rect::new(
                        content_area.right().saturating_sub(width),
                        content_area.bottom().saturating_sub(1),
                        width,
                        1,
                    );
                    regions.push(region(
                        LayoutRegionId::NewContentIndicator,
                        indicator,
                        request.viewport,
                    ));
                    interactions.push(InteractionRegion {
                        id: InteractionId::new("new-content"),
                        bounds: indicator,
                        clip: intersect(indicator, content_area),
                        intent: InteractionIntent::Invoke(ActionId::JumpEnd),
                    });
                }
            }
            if let Some(part_id) = request.fullscreen_artifact {
                regions.push(region(
                    LayoutRegionId::FullScreenArtifact,
                    geometry.primary,
                    request.viewport,
                ));
                interactions.push(InteractionRegion {
                    id: InteractionId::new(format!("fullscreen:{}", part_id.as_str())),
                    bounds: geometry.primary,
                    clip: intersect(geometry.primary, request.viewport),
                    intent: InteractionIntent::Scroll(ScrollRegionId::Artifact(part_id.clone())),
                });
            }
        } else {
            regions.push(region(
                LayoutRegionId::ResizeMessage,
                request.viewport,
                request.viewport,
            ));
        }

        let overlay = request.overlay.map(|overlay| {
            let overlay_rect = centered_overlay(request.viewport, class);
            let focus = FocusTarget::Overlay {
                overlay_id: overlay.id,
                control_id: ControlId::new("root"),
            };
            interactions.push(InteractionRegion {
                id: InteractionId::new(format!("overlay-backdrop:{}", overlay.id.as_u64())),
                bounds: request.viewport,
                clip: request.viewport,
                intent: InteractionIntent::Focus(focus.clone()),
            });
            interactions.push(InteractionRegion {
                id: InteractionId::new(format!("overlay:{}", overlay.id.as_u64())),
                bounds: overlay_rect,
                clip: intersect(overlay_rect, request.viewport),
                intent: InteractionIntent::Focus(focus),
            });
            interactions.push(InteractionRegion {
                id: InteractionId::new(format!("overlay-scroll:{}", overlay.id.as_u64())),
                bounds: overlay_rect,
                clip: intersect(overlay_rect, request.viewport),
                intent: InteractionIntent::Scroll(ScrollRegionId::Overlay(overlay.id)),
            });
            regions.push(region(
                LayoutRegionId::Overlay,
                overlay_rect,
                request.viewport,
            ));
            (overlay.id, overlay.kind.clone())
        });

        let focus_rect = focus_rect(request.focus, &regions, &transcript);
        let snapshot = LayoutSnapshot {
            generation: self.generation,
            viewport: request.viewport,
            class,
            route,
            overlay,
            regions,
            interactions,
            transcript,
            focus_rect,
        };
        debug_assert_eq!(snapshot.validate(), Ok(()));
        let invalidation = transition_between(self.previous.as_ref(), &snapshot);
        self.previous = Some(snapshot.clone());

        LayoutPass {
            snapshot,
            invalidation,
        }
    }
}

fn append_link_interactions(
    interactions: &mut Vec<InteractionRegion>,
    measured: &MeasuredPart,
    part: &super::snapshot::PartLayout,
    transcript_area: Rect,
) {
    let Some(markdown) = measured.markdown.as_ref() else {
        return;
    };
    for (index, link) in markdown.links.iter().enumerate() {
        let line = u32::try_from(link.line).unwrap_or(u32::MAX);
        if !part.clip_rows.contains(&line) {
            continue;
        }
        let row = u16::try_from(line.saturating_sub(part.clip_rows.start)).unwrap_or(u16::MAX);
        let start = u16::try_from(link.start_col)
            .unwrap_or(u16::MAX)
            .min(part.visible_rect.width);
        let end = u16::try_from(link.end_col)
            .unwrap_or(u16::MAX)
            .min(part.visible_rect.width);
        if start >= end {
            continue;
        }
        let bounds = Rect::new(
            part.visible_rect.x.saturating_add(start),
            part.visible_rect.y.saturating_add(row),
            end.saturating_sub(start),
            1,
        );
        interactions.push(InteractionRegion {
            id: InteractionId::new(format!(
                "link:{}:{index}:{}",
                part.part_id.as_str(),
                link.line
            )),
            bounds,
            clip: intersect(bounds, transcript_area),
            intent: InteractionIntent::OpenLink(link.url.clone()),
        });
    }
}

fn focus_rect(
    focus: &FocusTarget,
    regions: &[LayoutRegion],
    transcript: &TranscriptLayout,
) -> Option<Rect> {
    match focus {
        FocusTarget::Composer => find_region(regions, LayoutRegionId::ComposerField),
        FocusTarget::Transcript { part_id } => transcript
            .parts
            .iter()
            .find(|part| &part.part_id == part_id)
            .map(|part| part.visible_rect),
        FocusTarget::Artifact { part_id } => {
            find_region(regions, LayoutRegionId::FullScreenArtifact).or_else(|| {
                transcript
                    .parts
                    .iter()
                    .find(|part| &part.part_id == part_id)
                    .map(|part| part.visible_rect)
            })
        }
        FocusTarget::Overlay { .. } => find_region(regions, LayoutRegionId::Overlay),
        FocusTarget::DecisionDock => find_region(regions, LayoutRegionId::DecisionDock),
        FocusTarget::PlanDock => find_region(regions, LayoutRegionId::PlanDock),
        FocusTarget::PluginDock => find_region(regions, LayoutRegionId::PluginDock),
    }
}

fn find_region(regions: &[LayoutRegion], id: LayoutRegionId) -> Option<Rect> {
    regions
        .iter()
        .find(|region| region.id == id)
        .map(|region| region.rect)
}

fn region(id: LayoutRegionId, rect: Rect, viewport: Rect) -> LayoutRegion {
    LayoutRegion {
        id,
        rect,
        clip: intersect(rect, viewport),
    }
}

fn route_kind(route: &AppRoute) -> RouteLayoutKind {
    match route {
        AppRoute::Setup => RouteLayoutKind::Setup,
        AppRoute::Home => RouteLayoutKind::Home,
        AppRoute::Conversation { .. } => RouteLayoutKind::Conversation,
    }
}
