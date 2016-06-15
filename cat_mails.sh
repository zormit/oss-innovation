#!/bin/bash
files=( "gmane.comp.bug-tracking.flyspray.devel" "gmane.comp.emulators.freedos.devel" "gmane.comp.db.axion.devel" "gmane.comp.cad.geda.devel" "gmane.comp.desktop.xfce.devel.version4" "gmane.comp.boot-loaders.grub.devel" "gmane.comp.emulators.kvm.devel" )
# "gmane.comp.boot-loaders.u-boot"

for i in "${files[@]}"
do
    ls -v $i*.batch.mbox | xargs cat > $i.mbox
    mv $i*.batch.mbox ./batch
done
