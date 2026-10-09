package com.teragrep.zep_01.display;

public class FakeAngularObjectRegistryListener implements AngularObjectRegistryListener {

    @Override
    public void onAddAngularObject(final String interpreterGroupId, final AngularObject angularObject) {
        // no-op
        }

    @Override
    public void onUpdateAngularObject(final String interpreterGroupId, final AngularObject angularObject) {
        // no-op
        }

    @Override
    public void onRemoveAngularObject(final String interpreterGroupId, final AngularObject angularObject) {
        // no-op
        }
};