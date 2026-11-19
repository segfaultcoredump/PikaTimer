/*
 * Copyright (C) 2026 John Garner <segfaultcoredump@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.pikatimer.util;

import java.util.function.Function;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.ReadOnlyBooleanProperty;
import javafx.beans.property.ReadOnlyBooleanWrapper;
import javafx.beans.property.ReadOnlyObjectProperty;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.css.PseudoClass;
import javafx.scene.control.TextField;

/**
 * A TextField set up by {@link TextFieldFormatters}. It tracks, as you type,
 * whether the text is valid and what value it parses to, and flags the field
 * with the {@code :invalid} pseudo class (a red highlight) while it isn't.
 * <p>
 * Blank text is valid (with a null value) unless the field is required.
 *
 * @param <T> the parsed value type
 * @author John Garner <segfaultcoredump@gmail.com>
 */
public class ValidatedField<T> {

    public static final PseudoClass INVALID = PseudoClass.getPseudoClass("invalid");
    private static final String STYLESHEET = ValidatedField.class.getResource("text-field-validation.css").toExternalForm();

    private final TextField textField;
    private final Function<String, T> parser;
    private final ReadOnlyBooleanWrapper valid = new ReadOnlyBooleanWrapper(true);
    private final ReadOnlyObjectWrapper<T> value = new ReadOnlyObjectWrapper<>();
    private final BooleanProperty required = new SimpleBooleanProperty(false);

    /**
     * @param textField the field to watch
     * @param parser turns non-blank text into a value, or null if it isn't valid
     */
    ValidatedField(TextField textField, Function<String, T> parser) {
        this.textField = textField;
        this.parser = parser;

        // Put the stylesheet on the field itself so the highlight also works in
        // dialogs, which don't pick up the main window's stylesheets
        if (!textField.getStylesheets().contains(STYLESHEET)) textField.getStylesheets().add(STYLESHEET);

        valid.addListener((obs, o, n) -> textField.pseudoClassStateChanged(INVALID, !n));
        textField.textProperty().addListener((obs, o, n) -> revalidate());
        required.addListener((obs, o, n) -> revalidate());
    }

    /** Re-checks the current text. Called automatically when the text changes. */
    protected final void revalidate() {
        String text = textField.getText();
        T v = null;
        boolean ok;
        if (text == null || text.isBlank()) {
            ok = !required.get();
        } else {
            v = parser.apply(text);
            ok = v != null;
        }
        value.set(v);
        valid.set(ok);
        textField.pseudoClassStateChanged(INVALID, !ok);
    }

    public TextField getTextField() { return textField; }

    /** True when the text parses, or is blank and the field is not required. */
    public ReadOnlyBooleanProperty validProperty() { return valid.getReadOnlyProperty(); }
    public boolean isValid() { return valid.get(); }

    /** The parsed value, updated as you type. Null when blank or invalid. */
    public ReadOnlyObjectProperty<T> valueProperty() { return value.getReadOnlyProperty(); }
    public T getValue() { return value.get(); }

    /** When true, blank text is invalid. Defaults to false. */
    public BooleanProperty requiredProperty() { return required; }
    public boolean isRequired() { return required.get(); }
    public void setRequired(boolean r) { required.set(r); }
}
